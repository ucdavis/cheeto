# cheeto

Documentation for **cheeto**, the UC Davis HPC Core Facility's infrastructure
management CLI and services.

cheeto manages user and group provisioning, Slurm resource allocation, storage and automount
configuration, and LDAP synchronization for the HPCCF's clusters. MongoDB is the system of
record; the CLI and a set of scheduled celery tasks project that state into LDAP, Slurm's
accounting database, and puppet, and ingest provisioning events from HiPPO and person records
from UC Davis IAM.

Start with [Architecture](architecture/index.md) for the system overview, the external
integrations, and how the production deployment is laid out.

```{toctree}
:maxdepth: 2
:caption: Contents

architecture/index
user-guide/index
reference/index
```
