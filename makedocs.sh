set -e -o pipefail
kal_project_dir=`pwd`
source_dir=${kal_project_dir}
echo generating documents for ${kal_project_dir}
mkdir -p docs
cd ~
mkdir -p tmp
cd tmp
rm -rf adrdox
git clone https://github.com/adamdruppe/adrdox
cp ${kal_project_dir}/.skeleton.html adrdox/skeleton.html
cd adrdox
make
./doc2 -i ${source_dir}
mv generated-docs/* ${kal_project_dir}/docs
cp ${kal_project_dir}/docs/influxdb.html ${kal_project_dir}/docs/index.html
cd ${kal_project_dir}
echo succeeded - docs generated
