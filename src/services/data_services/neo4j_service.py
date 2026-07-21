from src.services.base_resource_service import BaseResourceService
from src.utils.logger import get_logger


# ========================= Neo4j 内部数据处理 ===========================
class Neo4jService(BaseResourceService):
    """
    Neo4j 内部数据处理
    """

    def __init__(self):
        self.logger = get_logger(f"{self.__class__.__name__}")

    async def delete_same_name_scholar(self):
        """删除同名学者"""
        delete_scholar_cypher = """
            MATCH(n:scholar)
            WITH n.university as university, n.name as name, count(n) as per_count
            WHERE per_count > 1
            MATCH(s:scholar)
            WHERE s.university = university and s.name = name
            DETACH DELETE s
            RETURN count(s) as del_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            delete_num = 0
            async with neo4j_driver.session() as session:
                result = await session.run(delete_scholar_cypher)
                record = await result.single()
                if record:
                    delete_num = record["del_count"]
            self.logger.info(f"执行同名同校学者清理，删除数量：{delete_num}")
            return delete_num
        except Exception as e:
            self.logger.error(f"删除同名学者失败: {str(e)}", exc_info=True)
            raise

    async def delete_same_name_company_inventor(self):
        """删除同名企业发明人"""
        delete_inventor_cypher = """
            MATCH(n:scholar)--(p:patent)--(c:company_inventor)
            WHERE n.name = c.name
            WITH collect(distinct c.name) as c_name_list
            MATCH(c2:company_inventor)
            WHERE c2.name in c_name
            DETACH DELETE c2
            RETURN count(c2) as del_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            delete_num = 0
            async with neo4j_driver.session() as session:
                result = await session.run(delete_inventor_cypher)
                record = await result.single()
                if record:
                    delete_num = record["del_count"]
            self.logger.info(f"执行同名同企业发明人清理，删除数量：{delete_num}")
            return delete_num
        except Exception as e:
            self.logger.error(f"删除同名企业发明人失败: {str(e)}", exc_info=True)
            raise

    async def create_patent_with(self):
        """创建学者合著关系"""
        patent_with_cypher = """
            MATCH (n1:scholar)-[r1:INVENTED_BY]-(p:patent)-[r2:INVENTED_BY]-(n2:scholar)
            // 排除自身关联（避免n1和n2是同一个学者的无效关系）
            where n1 <> n2
            // 2. 聚合统计：按n1.name和n2.name分组，统计共同专利数量
            WITH n1, n2, count(p) AS common_patent_count  // 推荐携带n1/n2节点（而非仅name），便于后续MERGE
            WHERE id(n1) < id(n2)  // 关键：强制按节点ID升序，避免n1/n2互换导致的重复关系
            // 3. 为n1和n2创建/更新PATENT_WITH关系
            MERGE (n1)-[r:PATENT_WITH]->(n2)
            // 4. 为关系设置属性（可包含共同专利数量、关系创建/更新时间等）
            ON CREATE SET r.common_patent_count = common_patent_count
            ON MATCH SET r.common_patent_count = r.common_patent_count + common_patent_count
            // 可选：返回结果，验证关系创建情况
            RETURN count(r) as patent_with_rel
        """
        # 删除跨校跨学院学者合著关系
        patent_with_delete = """
            match(s1:scholar)-[r:PATENT_WITH]-(s2:scholar)
            where s1.college <> s2.college or s1.university <> s2.university
            DELETE r
            RETURN count(r) as del_patent_with
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            create_num = 0
            del_num = 0

            async with neo4j_driver.session() as session:
                # 1. 先新增
                res_create = await session.run(patent_with_cypher)
                rec_create = await res_create.single()
                if rec_create:
                    create_num = rec_create["total"]
                self.logger.info(f"学者合著关系新增执行完成，数量：{create_num}")

                # 2. 后清理重复
                res_del = await session.run(patent_with_delete)
                rec_del = await res_del.single()
                if rec_del:
                    del_num = rec_del["total"]
                self.logger.info(f"学者合著关系冗余删除完成，数量：{del_num}")

            net = create_num - del_num
            self.logger.info(f"本次同步净新增学者合著关系：{net}")
            return net
        except Exception as e:
            self.logger.error(f"学者合著关系同步失败: {str(e)}", exc_info=True)
            raise

    async def college_community_detection(self, max_iter=3):
        """学院内社区划分"""
        self.logger.info("开始进行学院社区划分")
        try:
            #循环迭代拆分
            for i in range(1, max_iter + 1):
                self.logger.info(f"第 {i} 轮迭代...")

                # 获取超过20人的社区
                oversized = await self._find_oversize_community()

                if not oversized:
                    self.logger.info("所有社区 ≤20 人，停止迭代！")
                    break

                self.logger.info(f"需要拆分的大社区数量：{len(oversized)}")

                # 逐个拆分
                for communityId in oversized:
                    c_graph_name = str(communityId)
                    self.logger.info(f"对{c_graph_name}进行二次社区划分")
                    await self._leiden_community_detection(c_graph_name, communityId)

            self.logger.info(f"全部学院社区划分完成")
        except Exception as e:
            self.logger.error(f"社区划分失败: {str(e)}", exc_info=True)
            raise

    async def _drop_graph(self, graph_name):
        delete_old_graph = """
            CALL gds.graph.drop($graph_name, false) 
            YIELD graphName AS droppedGraph
            RETURN droppedGraph
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            async with neo4j_driver.session() as session:
                result = await session.run(delete_old_graph)
                record = await result.single()
                if record:
                    deleted_graph = record["droppedGraph"]
                    self.logger.info(f"删除图投影：{deleted_graph}")
                else:
                    self.logger.info(f"ℹ️ 图 {graph_name} 不存在，无需删除")
            return deleted_graph
        except Exception as e:
            self.logger.error(f"删除图投影失败: {str(e)}", exc_info=True)
            raise

    async def _find_oversize_community(self):
        # 查找大于20的社区
        self.logger.info(f"查找大于20人的社区")
        select_component = """
            match(s:scholar)
            where s.community is not null
            with s.community as communityId, count(distinct s) as per_count
            where per_count > 20
            return communityId
        """
        component_result = []
        try:
            neo4j_driver = await self.get_neo4j_driver()
            async with neo4j_driver.session() as session:
                result = await session.run(select_component)
                # 迭代读取必须在session存活范围内
                async for record in result:
                    component_result.append(record.data())
            self.logger.info(f"共获取到 {len(component_result)} 条大于20的社区")
            return component_result
        except Exception as e:
            self.logger.error(f"查找大于20的社区失败: {str(e)}", exc_info=True)
            raise

    async def _leiden_community_detection(self, graph_name, community_id):
        """
        对指定大社区进行Leiden子社区细分
        流程：删旧投影 -> 新建子图投影 -> Leiden算法划分 -> 重命名子社区 -> 清理临时字段
        :param graph_name: GDS临时投影图名称
        :param community_id: 父社区ID
        :return: 本次更新处理的学者节点数量
        """
        try:
            # 1. 删除旧图
            await self._drop_graph(graph_name)
            # 2. 创建新的图投影
            neo4j_driver = await self.get_neo4j_driver()
            update_count = 0

            async with neo4j_driver.session() as session:
                create_college_graph = """
                    MATCH (s1:scholar)-[r:PATENT_WITH]-(s2:scholar)
                    WHERE s1.community = $community_id
                      AND s2.community = $community_id
                    WITH gds.graph.project(
                        $graph_name,  // 参数化图名称
                        s1,
                        s2,
                        { relationshipProperties: { count: r.common_patent_count } },
                        { undirectedRelationshipTypes: ['*'] }
                    ) AS g
                    RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels;
                """
                res_create = await session.run(
                    create_college_graph,
                    {"community_id": community_id, "graph_name": graph_name}
                )
                record = await res_create.single()
                if record:
                    self.logger.info(
                        f"[{graph_name}] 子图投影创建成功："
                        f"节点数={record['nodes']}, 关系数={record['rels']}"
                    )
                else:
                    self.logger.warning(f"[{graph_name}] 无有效子图数据，跳过社区划分")
                    return 0

                # 3. 莱顿社区划分算法（所有子图均为连通图）
                self.logger.info(f"对{graph_name}图投影使用莱顿算法进行社区划分")
                college_graph_detection = """
                    CALL gds.leiden.write(
                        $graph_name,
                        {
                            gamma: 1.0,  // 默认1.0控制社区大小和均匀度
                            theta: 0.01,  // 随机程度， 默认0.01
                            relationshipWeightProperty: 'count',
                            writeProperty: 'ld_communityId'
                        }
                    )
                    YIELD nodePropertiesWritten, communityCount;
                """
                res_leiden = await session.run(college_graph_detection, {"graph_name": graph_name})
                leiden_record = await res_leiden.single()
                if leiden_record:
                    self.logger.info(
                        f"[{graph_name}] Leiden划分完成："
                        f"生成子社区数={leiden_record['communityCount']}, "
                        f"更新节点数={leiden_record['nodePropertiesWritten']}"
                    )
                # 4. 社区唯一命名
                community_rename_cypher = """
                    MATCH (s:scholar)
                    WHERE s.community = $community_id AND s.ld_communityId is not null               
                    // 拼接唯一社区标识（全部转字符串避免报错）
                    SET s.community = 
                        toString(s.community) + '_' +
                        toString(s.ld_communityId)             
                    // 删除旧字段
                    REMOVE s.ld_communityId
                    RETURN count(distinct s)
                """
                res_rename = await session.run(community_rename_cypher, {"community_id": community_id})
                rename_record = await res_rename.single()
                if rename_record:
                    update_count = rename_record["update_num"]

            # 5. 销毁临时投影图（放在session外不影响，纯资源清理）
            await self._drop_graph(graph_name)
            self.logger.info(f"父社区[{community_id}]子社区细分完成，共更新{update_count}个学者节点")

            return update_count

        except Exception as e:
            self.logger.error(
                f"Leiden社区划分失败 community_id={community_id}, graph_name={graph_name}: {str(e)}",
                exc_info=True
            )
            raise

    async def set_per_patent_count(self):
        patent_count = """
            MATCH (s:scholar)--(p:patent)
            WITH s, count(distinct p.application_number) as per_patent_count
            set s.per_patent_count = per_patent_count
            return count(distinct s) as scholar_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            scholar_count = 0
            async with neo4j_driver.session() as session:
                result = await session.run(patent_count)
                record = await result.single()
                if record:
                    scholar_count = record["scholar_count"]
                    self.logger.info(f"共设置{scholar_count}个学者个人专利数")
            return scholar_count
        except Exception as e:
            self.logger.error(f"设置每人专利数失败: {str(e)}", exc_info=True)
            raise

    async def set_community_patent_count(self):
        community_patent_count = """
            MATCH (s:scholar)--(p:patent)
            WHERE s.community IS NOT NULL
            WITH s.community as community_id, count(distinct p.application_number) as community_patent_count
            MATCH (s1:scholar)
            WHERE s1.community = community_id
            set s1.community_patent_count = community_patent_count
            return count(distinct s1.community) as community_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            community_count = 0
            async with neo4j_driver.session() as session:
                result = await session.run(community_patent_count)
                record = await result.single()
                if record:
                    community_count = record["community_count"]
                    self.logger.info(f"设置社区专利数成功，共{community_count}个社区")
            return community_count
        except Exception as e:
            self.logger.error(f"设置社区专利数失败: {str(e)}", exc_info=True)
            raise

    async def rename_community(self):
        rename_cypher = """
            MATCH (s:scholar)
            WHERE s.community IS NOT NULL
            WITH
                s.university as university,
                s.college AS college,
                s.community AS community,
                s.name AS scholar_name,
                s.per_patent_count AS per_patent_count
            // 排序选 leader
            ORDER BY community, per_patent_count DESC       
            // 最终聚合
            WITH
                university,
                college,
                community,
                collect(scholar_name)[0] AS leader_name
            WITH community, university + '_' + college + '_' + leader_name + '团队' as community_name
            MATCH (s1:scholar)
            WHERE s1.community = community
            SET s1.group_name = community_name
            RETURN count(distinct community_name) as community_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            community_count = 0
            async with neo4j_driver.session() as session:
                result = await session.run(rename_cypher)
                record = await result.single()
                if record:
                    community_count = record["community_count"]
                    self.logger.info(f"高校团队重命名完成，共{community_count}个团队")
            return community_count
        except Exception as e:
            self.logger.error(f"高校团队重命名失败: {str(e)}", exc_info=True)
            raise

    async def set_college_rank(self):
        college_rank = """
            MATCH (u:university)-[r1:CONTAINS]->(c:college)
            OPTIONAL MATCH (c)-[r2:CONTAINS]-(l:research_platform)
            OPTIONAL MATCH (c)-[r3:CONTAINS]-(s:subject)
            // 聚合实验室级别和学科（去重）
            WITH 
                u, c,
                collect(DISTINCT l.type) AS lab_level,
                collect(DISTINCT s.subject_name) AS A_subject
            // 计算各维度分值
            WITH 
                u, c,
                // 学校类型分值（985>211>双一流>一本）
                CASE u.type
                    WHEN '985' THEN 100
                    WHEN '211' THEN 80
                    WHEN '双一流' THEN 60
                    WHEN '一本' THEN 40
                    ELSE 0  // 其他类型默认0分
                END AS university_score,
                // 实验室分值（全国重点/国家研究中心 > 有实验室 > 无）
                CASE 
                    WHEN ANY(level IN lab_level WHERE level IN ['全国重点实验室', '国家研究中心']) THEN 100
                    WHEN size(lab_level) > 0 THEN 80  // 有其他类型实验室
                    ELSE 0  // 无实验室
                END AS lab_score,
                // 学科分值（有>无）
                CASE WHEN size(A_subject) > 0 THEN 100 ELSE 0 END AS subject_score
            // 计算总分并写入college节点的score属性
            WITH 
                c,
                round(
                    (lab_score * $lab_w) + 
                    (university_score * $university_w) + 
                    (subject_score * $subject_w), 
                    1
                ) AS college_score
            // 写入属性（若已存在则更新，不存在则新增）
            SET c.score = college_score
            // 返回更新结果
            RETURN count(distinct c) AS college_count
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            college_count = 0
            async with neo4j_driver.session() as session:
                result = await session.run(college_rank, {"lab_w": 0.5, "university_w": 0.2, "subject_w": 0.3})
                record = await result.single()
                if record:
                    college_count = record["college_count"]
                    self.logger.info(f"学院得分计算完成，共计算{college_count}个学院")
            return college_count
        except Exception as e:
            self.logger.error(f"学院得分计算失败: {str(e)}", exc_info=True)
            raise

    async def match_community(self, ipc_list):
        """
        根据ipc分类号匹配团队
        """
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        match_cypher = """
            # WITH $ipc_list AS prefixes
            MATCH (p:patent)-[r:INVENTED_BY]-(s:scholar)
            WHERE ANY(prefix IN prefixes WHERE p.main_cls_number STARTS WITH prefix)
            AND s.community is not null
            WITH s.university as university, s.college as college, s.community as community, collect(s.name) as scholar_list,  collect(distinct p.application_number) as patent_list
            MATCH (u:university {university: university})-[:CONTAINS]->(c:college {university: university, college: college})
            WHERE size(scholar_list) > 1
            // 匹配研究平台，先聚合去重
            OPTIONAL MATCH (c)-[:CONTAINS]->(rp:research_platform)
            WITH
                u,
                c,
                community,
                scholar_list,
                patent_list,
                // 先聚合研究平台，得到去重后的列表（消除rp不唯一的问题）
                COLLECT(DISTINCT CASE WHEN rp IS NOT NULL THEN {
                    name: rp.research_platform,
                    type: rp.type
                } END) AS research_platform_temp
            OPTIONAL MATCH (c)-[sr:CONTAINS]->(sj:subject)
            WITH 
                u.university as university,
                u.type as university_type,
                c.college as college,
                c.score as college_score,
                community,
                scholar_list,
                patent_list,
                research_platform_temp,
                // 聚合学科，得到去重后的列表（消除sj不唯一的问题）
                COLLECT(DISTINCT CASE WHEN sj IS NOT NULL THEN {
                    name: sj.subject_name,
                    type: sr.subject_level
                } END) AS subject_temp
            // 第三步：单独处理【学者-专利】计数（细粒度，无rp/sj干扰，避免笛卡尔积）
            MATCH (mp:patent)-[:INVENTED_BY]->(m:scholar)
            WHERE m.name IN scholar_list AND mp.application_number IN patent_list
            WITH
                university,
                university_type,
                college,
                college_score,
                community,
                scholar_list,
                patent_list,
                research_platform_temp,
                subject_temp,
                m.name AS scholar_name,
                // 此时无rp/sj干扰，COUNT计算精准
                COUNT(DISTINCT mp.application_number) AS per_patent_count
            // 第四步：聚合学者信息，过滤null值，返回最终结果
            WITH 
                university,
                university_type,
                college,
                college_score,
                community,
                scholar_list,
                patent_list,
                // 过滤研究平台中的null元素
                [x IN research_platform_temp WHERE x IS NOT NULL] AS research_platform,
                // 过滤学科中的null元素
                [x IN subject_temp WHERE x IS NOT NULL] AS subject,
                // 聚合学者列表（去重）
                COLLECT(DISTINCT {
                    name: scholar_name, 
                    patent_count: per_patent_count
                }) AS scholar_details
            
            RETURN 
                university,
                university_type,
                college,
                college_score,
                research_platform,
                subject,
                community,
                patent_list,
                scholar_details
            ORDER BY college_score DESC
        """
        try:
            neo4j_driver = await self.get_neo4j_driver()
            group_result = []
            async with neo4j_driver.session() as session:
                result = await session.run(match_cypher, {"ipc_list": ipc_list})
                async for record in result:
                    group_result.append(record.data())

            self.logger.info(f"企业IPC分类匹配社区完成")
            self.logger.info(f"共获取到 {len(group_result)} 条高校团队数据")
            return group_result
        except Exception as e:
            self.logger.error(f"企业IPC分类匹配社区失败: {str(e)}", exc_info=True)
            raise



# # ===================== 测试入口 =====================
# async def main():
#     service = Neo4jService()
#     await service.set_community_patent_count()
#
#
# if __name__ == '__main__':
#     import asyncio
#     asyncio.run(main())
