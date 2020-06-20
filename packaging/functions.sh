#!/bin/bash
#
# packaging/functions.sh
#
# Collection of functions useful during packaging
#
# Meant to be included from the main packaging/run.sh script (the variables used
# here are defined there, without which these functions won't really work).


function package_archive {
  local archive_name="${PACKAGE_NAME}-${VERSION}-${TARGET}"
  local archive_extension="tar.gz" # TODO support zip here once we package for windows too
  local archive_file="${PACKAGES_DIR}/${archive_name}.${archive_extension}"

  local temp_archive_dir="${TARGET_BUILD_DIR}/${archive_name}"

  if [ -d "${temp_archive_dir}" ]; then
    echo "Temporary archive directory ${temp_archive_dir} already exists. Removing it first"
    rm -rfv "$temp_archive_dir"
  fi
  mkdir -p "$temp_archive_dir"

  echo "Copying files to temporary archive directory: ${temp_archive_dir}"

  # main binary
  mkdir -p "${temp_archive_dir}/bin"
  cp -v "$TARGET_BIN" "${temp_archive_dir}/bin"

  # support files
  cp -v "${ROOT_DIR}/README.md" "${ROOT_DIR}/LICENSE" "${temp_archive_dir}/"
  cp -vR "${ROOT_DIR}/packaging/distribution/etc/" "${temp_archive_dir}/"
  # TODO enable this after some example cleanup
  #cp -vR "${ROOT_DIR}/demo/examples/" "${temp_archive_dir}/etc/tremor/config/"

  # tremor-script lib
  # TREMOR_PATH needs to be set to wherever this folder gets extracted to
  mkdir -p "${temp_archive_dir}/lib/tremor"
  cp -vR "${ROOT_DIR}/tremor-script/lib/" "${temp_archive_dir}/lib/tremor/tremor-script/"

  echo "Creating archive file: ${archive_file}"
  tar czf $archive_file -C "$TARGET_BUILD_DIR" "$archive_name"

  # final cleanup
  rm -rf "$temp_archive_dir"

  # print package details
  echo "PACKAGE SIZE:"
  du --human-readable --summarize "$archive_file" | awk '{print $1}'
  echo "PACKAGE INFO:"
  gzip --list "$archive_file"
  echo "PACKAGE CONTENTS:"
  tar --gzip --list --verbose --file="$archive_file"

  echo "Successfully built the archive file: ${archive_file}"
}


function package_deb {
  # install cargo-deb if not already there (helps us easily build a deb package)
  # see https://github.com/mmstick/cargo-deb
  if ! cargo deb --version > /dev/null 2>&1; then
    echo "Installing cargo-deb..."
    cargo install cargo-deb
  fi

  echo "Creating deb file in directory: ${PACKAGES_DIR}"
  # attempt to get the deb file name, but this suppresses error output too
  #local deb_file=$(cargo deb --no-build --no-strip --output "$PACKAGES_DIR" --deb-version "$VERSION" --target "$TARGET" | tail -n1)
  # we control stripping as part of the build process separately so don't do it here
  cargo deb --verbose --no-build --no-strip \
    --target "$TARGET" \
    --output "$PACKAGES_DIR" \
    --deb-version "$VERSION"

  # final cleanup. directory created by cargo-deb
  rm -rf "${ROOT_DIR}/target/${TARGET}/debian/"

  # print package details
  local deb_file="${PACKAGES_DIR}/*.deb"
  echo "PACKAGE SIZE:"
  du --human-readable --summarize $deb_file | awk '{print $1}'
  echo "PACKAGE INFO:"
  dpkg --info $deb_file
  echo "PACKAGE CONTENTS:"
  dpkg --contents $deb_file

  echo "Successfully built the deb file: ${deb_file}"
}


function package_rpm {
  local rpm_build_dir="${TARGET_BUILD_DIR}/rpmbuild"

  # install cargo-rpm if not already there (helps us easily build a rpm package)
  # https://github.com/iqlusioninc/cargo-rpm
  #
  # currently need to install it from the git develop branch for the various
  # rpm build flags to work (below). once v0.8.0 is released, install it from
  # crates.io. changes from develop branch that we make use of here:
  # https://github.com/iqlusioninc/cargo-rpm/pulls?q=is%3Apr+is%3Aclosed+author%3Aanupdhml
  if ! cargo rpm version > /dev/null 2>&1; then
    echo "Installing cargo-rpm..."
    cargo install --git https://github.com/iqlusioninc/cargo-rpm.git --branch develop
  fi

  echo "Creating rpm file in directory: ${PACKAGES_DIR}"
  cargo rpm build --verbose --no-cargo-build \
    --target "$TARGET" \
    --output "$PACKAGES_DIR"

  # final cleanup. directory created by cargo-rpm
  rm -rf "$rpm_build_dir"

  # print package details
  local rpm_file="${PACKAGES_DIR}/*.rpm"
  echo "PACKAGE SIZE:"
  du --human-readable --summarize $rpm_file | awk '{print $1}'
  echo "PACKAGE INFO:"
  rpm --query --info --verbose --package $rpm_file
  echo "PACKAGE REQUIREMENTS:"
  rpm --query --requires --verbose --package $rpm_file
  echo "PACKAGE CONTENTS:"
  rpm --query --list --verbose --package $rpm_file

  echo "Successfully built the rpm file: ${rpm_file}"
}
