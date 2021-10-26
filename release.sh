#!/usr/bin/env bash

set -e

TOML_FILES="\
Cargo.toml \
tremor-api/Cargo.toml \
tremor-cli/Cargo.toml \
tremor-common/Cargo.toml \
tremor-influx/Cargo.toml \
tremor-pipeline/Cargo.toml \
tremor-script/Cargo.toml \
tremor-value/Cargo.toml
"
VERSION_TESTS="\
tremor-cli/tests/api-cli/command.yml \
tremor-cli/tests/api/command.yml\
"
DOCKER_FILES="\
Dockerfile.learn\
"
PACKAGES="\
tremor-common \
tremor-value \
tremor-script\
"
old=$1
new=$2
branch=$3

if [ -z "${old}" ] || [ -z "${new}" ]
then
    echo "please run: $0 <old version> <new version> [<base branch>]"
    exit 1
fi

if [ -z "${branch}" ]
then
  branch="main"
fi

if [ "$(git status --porcelain=v1 2>/dev/null | wc -l)" -ne 0 ]
then
    git status
    echo "There are unsaved changes in the repository, press CTRL-C to abort now or return to continue."
    read -r answer
fi

echo -n "Release process from starting from '${old}' -> '${new}', do you want to continue? [y/N] "
read -r answer


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


echo "Testing the code ..."
cargo test --all

echo "Please review the following changes. (return to continue)"
read -r answer

git diff

echo "Do you want to Continue or Rollback? [c/R]"
read -r answer

case "${answer}" in
    C*|c*)
        git checkout -b "release-v${new}"
        git commit -sa -m "Rlease v${new}"
        git push --set-upstream origin "release-v${new}"
        ;;
    *)
        git checkout .
        exit
        ;;
esac;

echo "Please open the following pull request we'll wait here continue when it is merged."
echo
echo "  >> https://github.com/tremor-rs/tremor-runtime/pull/new/release-v${new} <<"
echo
echo "Once you continue we'll generate and push the release tag with the latest '${branch}'"
read -r answer

echo "Generating release tag v${new}"

git checkout "${branch}"
git pull
git tag -a -m"Release v${new}" "v${new}"
git push --tags

echo "Publishing packages"

for pkg in ${PACKAGES}
do
    cd "$pkg"
    if ! cargo test && cargo publish
    then
        echo "Package $pkg failed to publish - this could be OK if there was no change."
        echo "Do you want to Continue or Abort? [c/A]"
        read -r answer
        case "${answer}" in
            C*|c*)
                ;;
            *)
                exit1
                ;;
        esac;

    fi
    cd ..
done

echo "Preparing TLS"

echo "Do you want to update the Language Server?"
echo "Skip TLS or Update it? [s/U]"
read -r answer
case "${answer}" in
    U*|u*)
        mkdir -p temp
        cd temp
        git clone git@github.com:tremor-rs/tremor-language-server.git

        cd tremor-language-server

        toml="Cargo.toml"
        echo -n "Updating TOML files:"
        echo -n " ${toml}"
        sed -e "s/^version = \"${old}\"$/version = \"${new}\"/" -e "s/^tremor-script = \"${old}\"$/tremor-script = \"${new}\"/" -i.release "${toml}"
        echo "."

        echo "Running tests"
        cargo test --all


        echo "Please review the following changes. (return to continue)"
        read -r answer

        git diff

        echo "Do you want to Continue or Rollback? [c/R]"
        read -r answer

        case "${answer}" in
            C*|c*)
                git checkout -b "release-v${new}"
                git commit -sa -m "Rlease v${new}"
                git push --set-upstream origin "release-v${new}"
                ;;
            *)
                git checkout .
                cd ../..
                exit
                ;;
        esac;

        echo "Please open the following pull request we'll wait here continue when it is merged."
        echo
        echo "  >> https://github.com/tremor-rs/tremor-language-server/pull/new/release-v${new} <<"
        echo
        echo "Once you continue we'll generate and push the release tag with the latest '${branch}'"
        read -r answer

        echo "Generating release tag v${new}"

        git checkout "${branch}"
        git pull
        git tag -a -m"Release v${new}" "v${new}"
        git push --tags

        cd ../..            
        ;;
    *)
        exit1
        ;;
esac;

echo "Congrats release v${new} is done!"