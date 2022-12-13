for f in $(find ../tremor-pipeline/src/op/*/*.rs | grep -v ../tremor-pipeline/src/op/trickle | sed -e "s;../tremor-pipeline/src/op/;;g" -e 's;.rs$;;')
do
  name=$(echo ${f} | sed -e 's;/;::;g')
  file="../tremor-pipeline/src/op/${f}.rs"
  doc_name=$(echo ${f} | sed -e 's;^.*/;;g' -e 's/_/-/g')
  doc_file="operators/${doc_name}.md"
  echo "${doc_file}"
  echo "# ${name}\n" > "${doc_file}"
  cat "${file}" | grep '^//!' | sed -e 's;^//! ;;g' -e 's;^//!;;g' >> "${doc_file}"
done