#/usr/bin/env bash

TOML_FILES="\
Cargo.toml \
tremor-api/Cargo.toml \
tremor-cli/Cargo.toml \
tremor-common/Cargo.toml \
tremor-pipeline/Cargo.toml \
tremor-script/Cargo.toml\
"
VERSION_TESTS="\
tremor-cli/tests/api-cli/command.yml \
tremor-cli/tests/api/command.yml\
"
DOCKER_FILES="\
Dockerfile.learn\
"
old=$1
new=$2

if [ "$(git status --porcelain=v1 2>/dev/null | wc -l)" -ne 0 ]
then
    git status
    echo "There are unsaved changes in the repository, press CTRL-C to abort now or return to continue."
    read answer
fi

echo -n "Release process from starting from ${old} -> ${new}, do you want to continue? [y/N] " 
read  answer


case "${answer}" in
    Y*|y*)
        ;;
    *)
        echo "Aborting"
        exit 0
        ;;
esac;

echo "==> ${answer}"

echo -n "Updating TOML files:"
for toml in ${TOML_FILES}
do
    echo -n " ${toml}"
    sed -e "s/^version = \"${old}\"$/version = \"${new}\"/" -i.release "${toml}"
done
echo "."

echo -n "Updating Version Tests:"
for f in ${VERSION_TESTS}
do
    echo -n " ${f}"
    sed -e "s/- '{\"version\":\"${old}\"/- '{\"version\":\"${new}\"/" -i.release "${f}"
done
echo "."


echo -n "Updating Docker files:"
for f in ${DOCKER_FILES}
do
    echo -n " ${f}"
    sed -e "s;^FROM tremorproject/tremor:${old}$;FROM tremorproject/tremor:${new};" -i.release "${f}"
done
echo "."

echo "Updating CHANGELOG.md"
sed -e "s/^## Unreleased$/## ${new}/" -i.release "CHANGELOG.md"


echo "Please review the following changes. (return to continue)"
read answer

git diff

echo "Do you want to Continue or Rollback? [c/R]"
read answer

case "${answer}" in
    C*|c*)
        ;;
    *)
        git checkout .
        ;;
esac;