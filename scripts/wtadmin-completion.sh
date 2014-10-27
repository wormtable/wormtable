#
# Bash completion script for wtadmin based on 
# http://www.debian-administration.org/article/317/An_introduction_to_bash_completion_part_2
#
# The name of the table we are working with
WT_TABLE=""
# Global variables to hold indexes and columns
WT_INDEXES=""
WT_COLUMNS=""

__wt_get_indexes()
{
    WT_INDEXES=$( wtadmin ls $WT_TABLE | tail -n+12 | awk '{print $1}' )
    return 0
}

__wt_get_columns()
{
    WT_COLUMNS=$( wtadmin show $WT_TABLE | awk '{print $2 }' | tail -n+4 )
    return 0
}

_hist_command()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    __wt_get_indexes 
    local values="$WT_INDEXES"
    COMPREPLY=( $(compgen -W "${values}" -- ${cur}) ) 
    return 0
}

_dump_command()
{
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local prev="${COMP_WORDS[COMP_CWORD-1]}"
    local opts="--index --start --stop"
    local value=""
    if [ "$prev" == "--index" ]; then
        __wt_get_indexes
        value="$WT_INDEXES"
    else
        __wt_get_columns
        value="$opts $WT_COLUMNS"
    fi
    COMPREPLY=( $(compgen -W "${value}" -- ${cur}) ) 

}

_wtadmin() 
{
    local cur prev commands cmd 
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    commands="help show ls hist rm add dump"
    if [ $COMP_CWORD -eq 1 ]; then 
        COMPREPLY=($(compgen -W "${commands}" -- ${cur}))  
        return 0;
    elif [ $COMP_CWORD -eq 2 ]; then 
        _filedir -d
        return 0
    fi
    # if we get this far we're on subcommand completion.
    local cmd="${COMP_WORDS[1]}"
    WT_TABLE="${COMP_WORDS[2]}"
    case "${cmd}" in
	    hist)
            _hist_command
            return 0;
            ;;
        dump)
            _dump_command
            return 0
            ;;
        *)
            ;;
    esac

    return 0
}
complete -F _wtadmin wtadmin
