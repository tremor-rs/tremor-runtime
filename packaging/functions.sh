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
  mkdir -p "$temp_archive_dir/bin"
  cp -v "$TARGET_BIN" "${temp_archive_dir}/bin"

  # support files
  cp -v "${ROOT_DIR}/README.md" "${ROOT_DIR}/LICENSE" "${temp_archive_dir}/"
  cp -vR "${ROOT_DIR}/packaging/distribution/etc/" "${temp_archive_dir}/"
  # TODO enable this after some example cleanup
  #cp -vR "${ROOT_DIR}/demo/examples/" "${temp_archive_dir}/etc/tremor/config/"

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
