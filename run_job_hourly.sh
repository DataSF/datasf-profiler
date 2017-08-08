
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
    # echo "   -j, --python job file  --python job file- ie the job you wan to run; ex: grab_asset_fields_defs.py or upload_screendoor_responses"
    echo
    #echo "  - n  --name of job --- name of job you want to run- should match the jobs in the config file "
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
job_type=""

while getopts "h?:d:c:p:j:n:" opt; do
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
    #j)  python_job=$OPTARG
        ;;
    #n)  job_type=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))


#[ "$1" = "--" ] && shift
if [ -z "$path_to_main_dir" ]; then
    echo "*****You must enter a path to the main directory****"
    display_help
    exit 1
fi
if [ -z "$config_fn" ]; then
    echo "*****You must enter a config file to run the job****"
    display_help
    exit 1
fi
if [ -z "$python_path" ]; then
    echo "*****You must enter a path for python****"
    display_help
    exit 1
fi



config1="configs/"
config_dir=$path_to_main_dir$config1
pydev="pydev/"


#update the metadata fields
$python_job1='profile_datasets.py'
$job_type1="profile_datasets"
$python_job2='profile_fields.py'
$job_type2="profile_fields"
$hourly=1
$python_path $path_to_main_dir$pydev$python_job1 -c $config_fn -d $config_dir -n $job_type1 -t $hourly

$python_path $path_to_main_dir$pydev$python_job2 -c $config_fn -d $config_dir -n $job_type2
