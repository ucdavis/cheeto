# cheeto: HPCCF utilities.

`cheeto` provides a library and schemas for validating our puppet
YAML files, merging account data, and converting from the HiPPO
YAML format to the puppet format.

### Converting: `cheeto hippo-convert`

Validates a generated HiPPO YAML and converts it to our puppet format.
Optionally extracts the public key and writes it to `[USER].pub` in the
specified directory.

    usage: cheeto hippo-convert [-h] -i HIPPO_FILE [-o PUPPET_FILE] [--key-dir KEY_DIR]

    Convert HIPPO yaml to puppet.hpc format.

    options:
      -h, --help            show this help message and exit
      -i HIPPO_FILE, --hippo-file HIPPO_FILE
      -o PUPPET_FILE, --puppet-file PUPPET_FILE
      --key-dir KEY_DIR

### Validating: `cheeto validate-puppet`

Validates the specified puppet YAML files.
Optionally, deep-merge the files into the first supplied file.
By default, prints the validated (and optionally merged) YAML
to standard out.

    usage: cheeto validate-puppet [-h] [--merge] [--dump DUMP] files [files ...]

    positional arguments:
      files        YAML files to validate.

    options:
      -h, --help   show this help message and exit
      --merge      Merge the given YAML files before validation.
      --dump DUMP  Dump the validated YAML to the given file

### Generating: `cheeto nocloud-render`

Generates `nocloud-net` installation files combining a base template with disk-specific layouts defined on a per-host basis.

```bash
usage: cheeto nocloud-render [-h] [--templates-dir TEMPLATES_DIR] [--authorized-keys AUTHORIZED_KEYS]
                             [--output-dir OUTPUT_DIR] [--cobbler-ip COBBLER_IP]
                             [--puppet-environment PUPPET_ENVIRONMENT] [--puppet-ip PUPPET_IP]
                             [--puppet-fqdn PUPPET_FQDN]

options:
  -h, --help            show this help message and exit
  --templates-dir TEMPLATES_DIR, -t TEMPLATES_DIR
  --authorized-keys AUTHORIZED_KEYS, -k AUTHORIZED_KEYS
  --output-dir OUTPUT_DIR, -o OUTPUT_DIR
  --cobbler-ip COBBLER_IP, -c COBBLER_IP
  --puppet-environment PUPPET_ENVIRONMENT, -e PUPPET_ENVIRONMENT
  --puppet-ip PUPPET_IP
  --puppet-fqdn PUPPET_FQDN
```

Template layout structure:
```bash
templates/
templates/hosts/ # Required: individual HOSTNAME.j2 files
templates/layouts/ # Optional: additional templates with disk layouts
templates/snippets/ # Optional: snippets to be available (e.g. for --authorized-keys)
```

Simple template for a host with a single disk (`templates/hosts/HOSTNAME.j2`):
```jinja
{#- HOSTNAME
Any metadata you want to have as notes about the host.
-#}
{% extends "single_disk.yaml" %}
```

More complex host template that requires multiple drive layouts  (`templates/hosts/HOSTNAME.j2`):
```jinja
{#- HOSTNAME
Any metadata you want to have as notes about the host.
-#}
{%- extends "raid1_all_root.yaml" %}
{%- block disks %}
{{ super() }}
{%- include 'raid1_largest_scratch.yaml' %}
{%- endblock disks %}
```
