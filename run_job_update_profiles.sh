
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
    echo " ./run_job_update_profiles.sh -d /Users/j9/Desktop/datasf-profiler/ -c fieldConfig_profiler_desktop.yaml -t 1 -p /usr/local/bin/python"
    exit 1
}
# Initialize our own variables:
path_to_main_dir=""
config_fn=""
python_path=""
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
python_job1="profile_datasets.py"
job_type1="profile_datasets"
python_job2="profile_fields.py"
job_type2="profile_fields"

if [ "$hourly" == "1" ]; then
    echo "***this is hourly run***"
    $python_path $path_to_main_dir$pydev$python_job1 -c $config_fn -d $config_dir -n $job_type1 -t $hourly
else
    echo "***this is the daily run***"
    $python_path $path_to_main_dir$pydev$python_job1 -c $config_fn -d $config_dir -n $job_type1
fi
$python_path $path_to_main_dir$pydev$python_job2 -c $config_fn -d $config_dir -n $job_type2
