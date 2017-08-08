
#!/bin/bash
#
#bash script to run a particular python job, using a python path and config files


OPTIND=1         # Reset in case getopts has been used previously in the shell.

display_help() {
    echo "Usage: $0 [option...] {d}" >&2
    echo
    echo "   -d, --main_dir   -- main path to package files"
    echo
    echo "   -c, --config_file_for_job   ---   run the script to update asset fields using this config file"
    echo
    echo "   -p, --python path  -- path to python- ie run which python to find out"
    echo
    echo "   -t, --hourly  --hourly- ie run the job hourly or not"
    echo
    echo "***example usage: /home/ubuntu/metadata-mgmt-tool/run_job.sh -d /home/ubuntu/metadata-mgmt-tool/ -j grab_asset_field_defs.py -p /home/ubuntu/miniconda2/bin/python -c fieldConfig_grab_asset_field_defs_server.yaml"
    echo "***example usage: /home/ubuntu/metadata-mgmt-tool/run_job.sh -d /home/ubuntu/metadata-mgmt-tool/ -j grab_datadictionary_attachments_defs.py -p /home/ubuntu/miniconda2/bin/python -c fieldConfig_existing_datadicts_server.yaml"
    echo "***example usage: /home/ubuntu/metadata-mgmt-tool/run_job.sh -d /home/ubuntu/metadata-mgmt-tool/ -j upload_screendoor_responses.py -p /home/ubuntu/miniconda2/bin/python -c fieldConfig_import_wkbks_server.yaml"
    echo "***example usage: /home/ubuntu/metadata-mgmt-tool/run_job.sh -d /home/ubuntu/metadata-mgmt-tool/ -j generate_wkbks.py -p /home/ubuntu/miniconda2/bin/python -c fieldConfig_generate_wkbks_server.yaml"
    exit 1
}
# Initialize our own variables:
path_to_main_dir=""
config_file=""
python_path=""
python_job=""
hourly=""

while getopts "h?:d:c:p:t:" opt; do
    case "$opt" in
    h|\?)
        display_help
        exit 0
        ;;
    d)  path_to_main_dir=$OPTARG
        ;;
    c)  config_fn=$OPTARG
        ;;
    p)  python_path=$OPTARG
        ;;
    t)  hourly=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

echo `$hourly`
echo `$path_to_main_dir`
echo "****"

config1="configs/"
config_dir=$path_to_main_dir$config1
pydev="pydev/"


#update the metadata fields
#$python_path $path_to_main_dir$pydev$python_job -c $config_fn -d $config_dir -n $job_type
