RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

error() {
  printf "[ ${RED}*${NC} ] %s\n" "$1"
}

ok() {
  printf "[ ${GREEN}*${NC} ] %s\n" "$1"
}
