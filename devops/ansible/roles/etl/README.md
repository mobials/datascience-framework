etl
===
This role 

Requirements
------------
This role requires ETLs and dags to exist in the repo.
It also requires a requirements.txt for etl dependencies.

Role Variables
--------------
airflow.dags: "{{ app_deploy_directory }}/src/dashboard/dags/*"

Dependencies
------------
Depends on the airflow role 

Example Playbook
----------------
```
- hosts: localhost
  become: true
  roles:
    - etl
```

License
-------
BSD

Author Information
------------------
Jared Rietdyk, jrietdyk@mobials.com
