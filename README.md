# cheeto: HPCCF core utilities.

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
