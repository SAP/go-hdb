# LDAP and HANA Test Setup with Docker Compose

This README outlines the setup of an LDAP server and HANA database using Docker Compose. It includes commands for managing the Docker containers and a script for configuring the LDAP server.

## Docker Compose Commands

Use the following commands to manage the Docker containers:


1. **Start Containers**
   ```bash
   docker compose up -d
   ```

2. **Execute LDAP Configuration Script**
   ```bash
   docker compose exec ldap /ldapConfig.sh
   ```

3. **Shut Down Containers**
   ```bash
   docker compose down
   ```

## Services Overview

- OpenLDAP
- Hana Express

## Network Configuration

A network called `ldap_network` is defined in the Docker Compose file to allow communication between the services.

## LDAP Setup Script

The `ldapConfig.sh` script creates the necessary organizational units, users, and groups within the LDAP directory.

## Conclusion

This setup allows you to run an LDAP server and a HANA database seamlessly using Docker Compose to support LDAP testing.
