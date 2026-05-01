create schema if not exists GCPMigrationmMeta;
use GCPMigrationmMeta;

create table config_table(
id bigint primary Key auto_increment,
table_name varchar(50) not null,
table_key char(40) as (sha1(table_name)) stored,

-- source and target
source_project varchar(150) not null default "datamigrationproject-494118",
source_dataset varchar(50) not null,
gcs_path varchar(500) null,
target_path varchar(500) not null,

-- control flags
active_flag tinyint not null default 1,
load_flag tinyint not null default 0,

-- two stages status
bq_to_gcs_status enum('NOT_STARTED','IN_PROGRESS','COMPLETED','FAILED') default 'NOT_STARTED',
gcs_to_bronze_status enum('NOT_STARTED','IN_PROGRESS','COMPLETED','FAILED') default 'NOT_STARTED',

-- common timestamp
last_run_ts datetime null,
last_success_ts datetime null,
error_message text null,

created_ts datetime not null default current_timestamp,
updated_ts datetime null on update current_timestamp,

unique key uk_table_name (table_name)
);

select * from config_table;

drop table config_table;
