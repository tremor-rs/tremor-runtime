for f in $(find ../src/postprocessor/*.rs | sed -e "s;../src/postprocessor/;;g" -e 's;.rs$;;')
do
  name=$(echo ${f} | sed -e 's/_/-/g')
  file="../src/postprocessor/${f}.rs"
  doc_name=$(echo ${f} | sed -e 's;^.*/;;g')
  doc_file="postprocessors/${doc_name}.md"
  echo "${doc_file}"
  echo "# ${name}\n" > "${doc_file}"
  cat "${file}" | grep '^//!' | sed -e 's;^//! ;;g' -e 's;^//!;;g' >> "${doc_file}"
done