for f in $(find ../src/preprocessor/*.rs | sed -e "s;../src/preprocessor/;;g" -e 's;.rs$;;')
do
  name=$(echo ${f} | sed -e 's/_/-/g')
  file="../src/preprocessor/${f}.rs"
  doc_name=$(echo ${f} | sed -e 's;^.*/;;g')
  doc_file="preprocessors/${doc_name}.md"
  echo "${doc_file}"
  echo "# ${name}\n" > "${doc_file}"
  cat "${file}" | grep '^//!' | sed -e 's;^//! ;;g' -e 's;^//!;;g' >> "${doc_file}"
done