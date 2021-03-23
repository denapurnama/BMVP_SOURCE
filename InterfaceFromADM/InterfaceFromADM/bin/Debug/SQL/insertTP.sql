IF OBJECT_ID('tempdb..##tempTP') IS NOT NULL
	DROP TABLE ##tempTP
create table ##tempTP(
	PROCESS_ID varchar (20),
	PROCESS_KEY varchar(65),
    SYSTEM_SOURCE varchar(20),
    CLIENT_ID varchar(3),
    MOV_TYPE varchar(3),
    DOC_DT varchar(10),
    POSTING_DT varchar(10),
    REF_NO varchar(16),
    MAT_DOC_DESC varchar(255),
    SND_PART_NO varchar(23),
    SND_PROD_PURPOSE_CD varchar(5),
    SND_SOURCE_TYPE varchar(1),
    SND_PLANT_CD varchar(4),
    SND_SLOC_CD varchar(6),
    SND_BATCH_NO varchar(10),
    RCV_PART_NO varchar(23),
    RCV_PROD_PURPOSE_CD varchar(5),
    RCV_SOURCE_TYPE varchar(1),
    RCV_PLANT_CD varchar(4),
    RCV_SLOC_CD varchar(6),
    RCV_BATCH_NO varchar(10),
    QUANTITY varchar(16),
    UNIT_OF_MEASURE_CD varchar(3),
    DN_COMPLETE_FLAG varchar(1),
    CREATED_BY varchar(20),
    CREATED_DT varchar(19)
)

insert into ##tempTP
select * from TB_T_TRANSFER_POSTING_TO_ICS

delete from TB_T_TRANSFER_POSTING_TO_ICS

insert into TB_T_TRANSFER_POSTING_TO_ICS
select * from ##tempTP as a
where (len(a.SYSTEM_SOURCE) > 0 and a.SYSTEM_SOURCE is not null)
		and (len(a.PROCESS_KEY) > 0 and a.PROCESS_KEY is not null)
		and (len(a.CLIENT_ID) > 0 and a.CLIENT_ID is not null)
		and (len(a.MOV_TYPE) > 0 and a.MOV_TYPE is not null)
		and (len(a.DOC_DT) > 0 and a.DOC_DT is not null)
		and (len(a.POSTING_DT) > 0 and a.POSTING_DT is not null)
		and (len(a.REF_NO) > 0 and a.REF_NO is not null)
		and (len(a.MAT_DOC_DESC) > 0 and a.MAT_DOC_DESC is not null)
		and (len(a.SYSTEM_SOURCE) > 0 and a.SYSTEM_SOURCE is not null)
		and (len(a.SND_PART_NO) > 0 and a.SND_PART_NO is not null)
		and (len(a.SND_PROD_PURPOSE_CD) > 0 and a.SND_PROD_PURPOSE_CD is not null)
		and (len(a.SND_SOURCE_TYPE) > 0 and a.SND_SOURCE_TYPE is not null)
		and (len(a.SND_PLANT_CD) > 0 and a.SND_PLANT_CD is not null)
		and (len(a.SND_SLOC_CD) > 0 and a.SND_SLOC_CD is not null)
		and (len(a.RCV_PART_NO) > 0 and a.RCV_PART_NO is not null)
		and (len(a.RCV_PROD_PURPOSE_CD) > 0 and a.RCV_PROD_PURPOSE_CD is not null)
		and (len(a.RCV_SOURCE_TYPE) > 0 and a.RCV_SOURCE_TYPE is not null)
		and (len(a.RCV_PLANT_CD) > 0 and a.RCV_PLANT_CD is not null)
		and (len(a.RCV_SLOC_CD) > 0 and a.RCV_SLOC_CD is not null)
		and (len(a.QUANTITY) > 0 and a.QUANTITY is not null)
		and (len(a.DN_COMPLETE_FLAG) > 0 and a.DN_COMPLETE_FLAG is not null)
		and (len(a.CREATED_BY) > 0 and a.CREATED_BY is not null)
		and (len(a.CREATED_DT) > 0 and a.CREATED_DT is not null)

select * from TB_T_TRANSFER_POSTING_TO_ICS