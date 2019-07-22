DROP DATABASE IF EXISTS :project_name ;
CREATE DATABASE :project_name ;
ALTER DATABASE :project_name OWNER to :project_name ;
ALTER USER :project_name WITH SUPERUSER;
