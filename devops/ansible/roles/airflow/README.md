airflow
=======
Install airflow as a service.

Requirements
------------
This role has a requirement on postgres being available for airflow.

Role Variables
--------------
The airflow variable needs to be set:

airflow:
  pip_package: The airflow package to install - "apache-airflow[async,aws,postgres]"
  home: Location to install airflow - "/etc/airflow"
  db: The credentials to login to postgres - postgresql://airflow:airflow@localhost:5432/airflow
  dags: The location of the dags to copy to airflow - "{{ app_deploy_directory }}/src/dashboard/dags/*"

Dependencies
------------
This role has a soft dependency on geerlingguy.postgresql, it is not 100% needed but we are using it for our local db in most cases.

Example Playbook
----------------
```
- hosts: localhost
  become: true
  roles:
    - airflow
```

License
-------
BSD

Author Information
------------------
Jared Rietdyk, jrietdyk@mobials.com
