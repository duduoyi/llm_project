from sqlalchemy import Column, String, Text, Date, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy.dialects.mysql import INTEGER


# 初始化 SQLAlchemy ORM 基础基类（仅原生 Base，无自定义子类拆分）
Base = declarative_base()


class DimUniversityInfo(Base):
    """
    大学信息表 ORM 模型
    来源：手动整理(理工科985、211、双一流)，EXCEL上传
    对应数据库表：dim_university_info
    """
    __tablename__ = "dim_university_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    university = Column(String(50), nullable=False, comment="大学名称")
    province = Column(String(10), nullable=False, comment="省份")
    city = Column(String(15), nullable=False, comment="城市")
    type = Column(String(5), nullable=False, comment="大学类型")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")


class OdsUniversityPatentInfo(Base):
    """
    高校专利信息表 ORM 模型
    来源：国家知识产权局，类型：发明授权
    对应数据库表：ods_university_patent_info
    """
    __tablename__ = "ods_university_patent_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    title = Column(String(255), default=None, comment="标题")
    application_number = Column(String(16), nullable=False, comment="申请专利号")
    publication_number = Column(String(255), default=None, comment="授权公布号")
    publication_date = Column(Date, nullable=False, comment="公开日")
    application_date = Column(Date, nullable=False, comment="申请日")
    main_cls_number = Column(String(255), default=None, comment="主分类号")
    inventor = Column(Text, default=None, comment="发明人")
    summary = Column(Text, nullable=False, comment="摘要")
    applicant = Column(String(255), nullable=False, comment="申请人")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    # 索引说明：SQLAlchemy 中，唯一索引/普通索引可在此声明（也可通过迁移工具创建）
    __table_args__ = (
        UniqueConstraint("application_number", name="uk_application_number"),
        Index("idx_create_time", "create_time"),
        Index("index_publication_date", "publication_date"),
    )

class DimIPCStandardCollege(Base):
    """
    专利大类对应学院信息表 ORM 模型
    来源：个人整理
    对应数据库表：dim_ipc_standard_college
    """
    __tablename__ = "dim_ipc_standard_college"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    ipc_type = Column(String(3), nullable=False, comment="IPC大类")
    subject_code = Column(String(5), nullable=False, comment="学科代码")
    subject_code = Column(String(20), nullable=False, comment="学科名")
    standard_college = Column(String(20), nullable=False, comment="标准类型学院")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    # 索引说明：SQLAlchemy 中，唯一索引/普通索引可在此声明（也可通过迁移工具创建）
    __table_args__ = (
        UniqueConstraint("ipc_type", name="ac_ipc"),
    )


class DimUniversityScholar(Base):
    """
    大学学者信息表 ORM 模型
    来源：大学学院教师名单
    对应数据库表：dim_university_scholar
    """
    __tablename__ = "dim_university_scholar"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    name = Column(String(50), nullable=False, comment="学者姓名")
    university = Column(String(50), nullable=False, comment="大学名称")
    college = Column(String(15), nullable=False, comment="学院名称")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    __table_args__ = (
        UniqueConstraint("university", "college", "name", name="uk_school_college_name"),
        Index("idx_create_time", "create_time"),
        Index("idx_school_name", "university", "name"),
    )

class DimStandardCollege(Base):
    """
    实际学院名称对应标准类型学院信息表 ORM 模型
    来源：个人整理
    对应数据库表：dim_standard_college
    """
    __tablename__ = "dim_standard_college"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    actual_college = Column(String(20), nullable=False, comment="实际学院名")
    standard_college = Column(String(20), nullable=False, comment="标准类型学院")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    # 索引说明：SQLAlchemy 中，唯一索引/普通索引可在此声明（也可通过迁移工具创建）
    __table_args__ = (
        UniqueConstraint("actual_college", name="ac_college"),
    )

class DimUniversitySubject(Base):
    """
    大学重点学科信息表 ORM 模型
    来源：大学学院教师名单
    对应数据库表：dim_university_subject
    """
    __tablename__ = "dim_university_subject"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    subject_code = Column(String(5), nullable=False, comment="学科代码")
    subject_name = Column(String(20), nullable=False, comment="学科名称")
    subject_level = Column(String(5), nullable=False, comment="学科等级")
    subject_type = Column(String(10), nullable=False, comment="学科类型")
    university = Column(String(20), nullable=False, comment="大学名称")
    college = Column(String(20), nullable=False, comment="学院名称")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    __table_args__ = (
        UniqueConstraint("university", "subject_name", name="uk_school_subject"),
        Index("idx_create_time", "create_time"),
    )


class DimUniversityResearchPlatform(Base):
    """
    大学科研平台信息表 ORM 模型
    来源：百度
    对应数据库表：dim_university_research_platform
    """
    __tablename__ = "dim_university_research_platform"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    research_platform = Column(String(50), nullable=False, comment="科研平台名称")
    type = Column(String(20), nullable=False, comment="科研平台类型")
    university = Column(String(50), nullable=False, comment="大学名称")
    college = Column(String(15), nullable=False, comment="学院名称")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    __table_args__ = (
        UniqueConstraint("university", "research_platform", "college", name="uk_school_lab_college"),
        Index("idx_create_time", "create_time"),
    )


class DimUniversityParkInfo(Base):
    """
    大学科技园信息表 ORM 模型
    来源：手动整理，EXCEL上传
    对应数据库表：dim_university_park_info
    """
    __tablename__ = "dim_university_park_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    university_park = Column(String(50), nullable=False, comment="大学科技园名称")
    province = Column(String(10), nullable=False, comment="省份")
    city = Column(String(15), nullable=False, comment="城市")
    university = Column(String(50), nullable=False, comment="大学名称")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")


class OdsCompanyInfo(Base):
    """
    企业信息表 ORM 模型
    来源：企查查，类型：上市/独角兽/专精特新/高企/有专利，地区：昆山市/海门区/太仓市/南通市
    对应数据库表：dim_company_info
    """
    __tablename__ = "dim_company_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    company_name = Column(String(50), name="企业名称", nullable=False, comment="企业名称")
    reg_status = Column(String(5), name="登记状态", nullable=False, comment="登记状态")
    legal_representative = Column(String(255), name="法定代表人", nullable=False, comment="法定代表人")
    reg_capital = Column(String(20), name="注册资本", default=None, comment="注册资本")
    start_date = Column(Date, name="成立日期", nullable=False, comment="成立日期")
    company_code = Column(String(18), name="统一社会信用代码", nullable=False, comment="统一社会信用代码")
    reg_address = Column(String(255), name="企业注册地址", default=None, comment="企业注册地址")
    province = Column(String(10), name="所属省份", nullable=False, comment="所属省份")
    city = Column(String(20), name="所属城市", nullable=False, comment="所属城市")
    county = Column(String(20), name="所属区县", nullable=False, comment="所属区县")
    valid_phone = Column(String(255), name="有效手机号", default=None, comment="有效手机号")
    more_phones = Column(Text, name="更多电话", default=None, comment="更多电话")
    email = Column(String(255), name="邮箱", default=None, comment="邮箱")
    company_type = Column(String(50), name="企业(机构)类型", default=None, comment="企业(机构)类型")
    taxpayer_identification_number = Column(String(18), name="纳税人识别号", comment="纳税人识别号")
    reg_number = Column(String(15), name="注册号", default=None, comment="注册号")
    organization_code = Column(String(10), name="组织机构代码", default=None, comment="组织机构代码")
    staff_size = Column(INTEGER(unsigned=True), name="参保人数", default=None, comment="参保人数")
    staff_size_report = Column(String(4), name="参保人数所属年报", default=None, comment="参保人数所属年报")
    approval_date = Column(Date, name="核准日期", default=None, comment="核准日期")
    business_term = Column(String(30), name="营业期限", default=None, comment="营业期限")
    national_industry_category = Column(String(30), name="国标行业门类", default=None, comment="国标行业门类")
    national_industry_major = Column(String(30), name="国标行业大类", default=None, comment="国标行业大类")
    national_industry_medium = Column(String(30), name="国标行业中类", default=None, comment="国标行业中类")
    national_industry_minor = Column(String(30), name="国标行业小类", default=None, comment="国标行业小类")
    company_scale = Column(String(6), name="企业规模", default=None, comment="企业规模")
    former_names = Column(Text, name="曾用名", default=None, comment="曾用名")
    english_name = Column(String(255), name="英文名", default=None, comment="英文名")
    official_website = Column(Text, name="官网网址", default=None, comment="官网网址")
    mailing_address = Column(String(255), name="通信地址", default=None, comment="通信地址" )
    company_honor = Column(String(10), name="企业荣誉", nullable=False, comment="企业荣誉")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    # 索引与联合约束：使用模型的英文属性名（SQLAlchemy 会自动映射到数据库中文列名）
    __table_args__ = (
        UniqueConstraint("统一社会信用代码", "企业荣誉", name="uk_code_title"),
        Index("idx_create_time", "create_time"),
    )


class OdsCompanyPatentInfo(Base):
    """
    企业专利信息表 ORM 模型
    来源：国家知识产权局，类型：发明授权
    对应数据库表：ods_company_patent_info
    """
    __tablename__ = "ods_company_patent_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    title = Column(String(255), default=None, comment="标题")
    application_number = Column(String(16), nullable=False, comment="申请专利号")
    publication_number = Column(String(255), default=None, comment="授权公布号")
    publication_date = Column(Date, nullable=False, comment="公开日")
    application_date = Column(Date, nullable=False, comment="申请日")
    main_cls_number = Column(String(255), default=None, comment="主分类号")
    inventor = Column(Text, default=None, comment="发明人")
    summary = Column(Text, nullable=False, comment="摘要")
    applicant = Column(String(255), nullable=False, comment="申请人")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")

    # 索引说明：SQLAlchemy 中，唯一索引/普通索引可在此声明（也可通过迁移工具创建）
    __table_args__ = (
        UniqueConstraint("application_number", name="uk_application_number"),
        Index("idx_create_time", "create_time"),
        Index("index_publication_date", "publication_date"),
    )


class DimAreaInfo(Base):
    """
    开发区信息表 ORM 模型
    来源：手动整理，EXCEL上传
    对应数据库表：dim_area_info
    """
    __tablename__ = "dim_area_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    area = Column(String(50), nullable=False, comment="开发区名称")
    type = Column(String(10), nullable=False, comment="开发区类型")
    province = Column(String(10), nullable=False, comment="省份")
    city = Column(String(15), nullable=False, comment="城市")
    county = Column(String(15), nullable=False, comment="区县")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")


class DimGovernmentInfo(Base):
    """
    政府信息表 ORM 模型
    来源：手动整理，EXCEL上传
    对应数据库表：dim_government_info
    """
    __tablename__ = "dim_government_info"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    government_name = Column(String(50), nullable=False, comment="政府名称")
    province = Column(String(10), nullable=False, comment="省份")
    city = Column(String(15), nullable=False, comment="城市")
    county = Column(String(15), nullable=False, comment="区县")
    area = Column(String(50), default=None, comment="开发区")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")


class OdsETLRun(Base):
    """
    ETL状态表 ORM 模型
    来源：fastAPI后端ETL流程生成
    对应数据库表：ods_etl_run
    """
    __tablename__ = "ods_etl_run"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    run_id = Column(String(64), primary_key=True, nullable=False, comment="运行id")
    status = Column(String(20), nullable=False, comment="ETL服务状态")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")
    update_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="更新时间（数据库自动生成）")


class OdsETLBatchCheckpoint(Base):
    """
    ETL批次状态表 ORM 模型
    来源：fastAPI后端ETL流程生成
    对应数据库表：ods_etl_run
    """
    __tablename__ = "ods_etl_batch_checkpoint"  # 严格匹配数据库表名

    # 字段定义：严格对应表结构，包含约束、类型、注释
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True, comment="自增主键ID")
    run_id = Column(String(64), nullable=False, comment="运行id")
    batch_no = Column(INTEGER(unsigned=True), comment="批次")
    last_id = Column(INTEGER(unsigned=True), comment="开始id")
    batch_size = Column(INTEGER(unsigned=True), comment="批次数量")
    status = Column(String(10), nullable=False, comment="批次状态") # pending/running/success/failed
    retry_count = Column(INTEGER(unsigned=True), default=0, comment="重试次数")
    error_msg = Column(Text, comment="报错内容")
    create_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="创建时间（数据库自动生成）")
    update_time = Column(TIMESTAMP, nullable=False, server_default="CURRENT_TIMESTAMP", comment="更新时间（数据库自动生成）")

    # 索引说明：SQLAlchemy 中，唯一索引/普通索引可在此声明（也可通过迁移工具创建）
    __table_args__ = (
        UniqueConstraint("run_id", "batch_no", name="uniq_run_batch"),
    )
