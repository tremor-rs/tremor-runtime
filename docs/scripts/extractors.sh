for f in $(find ../tremor-script/src/extractor/*.rs | grep -v "extractor/test.rs" | sed -e "s;../tremor-script/src/extractor/;;g" -e 's;.rs$;;')
do
  name=$(echo ${f} | sed -e 's/_/-/g')
  file="../tremor-script/src/extractor/${f}.rs"
  doc_name=$(echo ${f} | sed -e 's;^.*/;;g')
  doc_file="extractors/${doc_name}.md"
  echo "${doc_file}"
  echo "# ${name}\n" > "${doc_file}"
  cat "${file}" | grep '^//!' | sed -e 's;^//! ;;g' -e 's;^//!;;g' >> "${doc_file}"
done