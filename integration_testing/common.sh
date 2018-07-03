RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

error() {
  printf "[ ${RED}ERR${NC} ] %s\n" "$1"
}

ok() {
  printf "[ ${GREEN}OK ${NC} ] %s\n" "$1"
}
