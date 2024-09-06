SCRIPT_PATH="${BASH_SOURCE:-$0}"
ABS_SCRIPT_PATH="$(realpath "${SCRIPT_PATH}")"
ABS_DIR="$(dirname "${ABS_SCRIPT_PATH}")"

java -classpath "$ABS_DIR/synclite-validator.jar:$ABS_DIR/*" com.synclite.validator.Main "$@"