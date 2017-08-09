

#!/bin/bash
#
#bash script to run a particular python job, using a python path and config files


OPTIND=1         # Reset in case getopts has been used previously in the shell.
display_help() {
    echo
    echo "Usage: $0 [option...] {d}" >&2
    echo
    echo "   -d, --main_dir   -- main path to package files"
    echo
    echo "   -c, --config_file_for_job   ---   run the script to update asset fields using this config file"
    echo
    echo "   -p, --python_path  -- path to python- ie run which python to find out"
    echo
    echo "   -t, --hourly  --hourly- ie run the job hourly or not- You must enter a Y for this option"
    echo
    echo "   -f, --profile_fields --profile fields- ie run the the profile fields job- accepts Y to run the job"
    echo
    echo "   -m, --profile_datasets --profile datasets- ie run the the profile datasets job- accepts Y to run the job"
    echo
    echo "   -h,  --help enter -h to display this help message"
    echo
    echo "  EX: ./run_job_update_profiles.sh -d /Users/j9/Desktop/datasf-profiler/ -c fieldConfig_profiler_desktop.yaml -p /usr/local/bin/python -t Y -f Y -m Y  "
    echo
    exit 1
}


# Initialize our own variables:
path_to_main_dir=""
config_fn=""
python_path=""
hourly="N"
profile_fields="N"
profile_datasets="N"
while getopts "h?:d:c:p:t:f:m:" opt; do
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
    f)  profile_fields=$OPTARG
        ;;
    m) profile_datasets=$OPTARG
    esac
done

shift $((OPTIND-1))



#[ "$1" = "--" ] && shift
if [ -z "$path_to_main_dir" ]; then
    echo
    echo "*****You must enter a path to the main directory****"
    display_help
    exit 1
fi
if [ -z "$config_fn" ]; then
    echo
    echo "*****You must enter a config file to run the job****"
    display_help
    exit 1
fi
if [ -z "$python_path" ]; then
    echo "*****You must enter a path for python****"
    display_help
    exit 1
fi
if [ "$profile_fields" != "N" ]; then
    if [ "$profile_fields" != "Y" ]; then
        echo
        echo "*** ERROR: Invalid input: You must enter a Y to run the profile fields job"
        display_help
        exit 1
    fi
fi
if [ "$profile_datasets" != "N" ]; then
    if [ "$profile_datasets" != "Y" ]; then
        echo
        echo "*** ERROR: Invalid input: You must enter a Y to run the profile datasets job"
        display_help
        exit 1
    fi
fi
if [ "$hourly" != "N" ] && [ "$hourly" != "Y" ]; then
        echo
        echo "*** ERROR: Invalid input: You must enter a Y to run the profile datasets job hourly"
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



if [ "$hourly" == "Y" ] && [ "$profile_datasets" == "Y" ]; then
    hourly="1"
    echo
    echo "***PROFILING DATASETS...****"
    echo "****This is hourly profile datasets run***"
    $python_path $path_to_main_dir$pydev$python_job1 -c $config_fn -d $config_dir -n $job_type1 -t $hourly
    exit 0
fi
if [ "$profile_datasets" == "Y" ] && [ "$hourly" == "N" ]; then
    echo
    echo "***PROFILING DATASETS...****"
    echo "***This is the daily run of profile datasets***"
    $python_path $path_to_main_dir$pydev$python_job1 -c $config_fn -d $config_dir -n $job_type1
    exit 0
fi
if [ "$profile_fields" == "Y" ]; then
    echo
    echo  "***PROFILING FIELDS...****"
    $python_path $path_to_main_dir$pydev$python_job2 -c $config_fn -d $config_dir -n $job_type2
    exit 0
fi
if [ "$profile_fields" == "N" ] && [ "$profile_datasets" == "N" ]; then
    echo
    echo "*****YOU DID NOT SELECT ANY PROFILING JOBS TO RUN!******"
    echo "******** Are you sure?******** "
    display_help
    exit 0
fi
