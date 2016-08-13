#!/usr/bin/env bash

echo "REVISION = '$(git rev-parse HEAD)'" > pysyncobj/revision.py

rm -rf pysyncobj/pysyncobj3/
2to3 --output-dir=pysyncobj3 -W -n pysyncobj
mv pysyncobj3/ pysyncobj/pysyncobj3/
mv pysyncobj/pysyncobj3/py3init.py pysyncobj/pysyncobj3/__init__.py
cp test_syncobj.py test_syncobj3.py
2to3 -w test_syncobj3.py

for f in pysyncobj/pysyncobj3/*
do
  sed 's/\\/\\\\/g' $f > tmp
  mv tmp $f
  echo -e "#\n#  WARNING: this is generated file, use generate.sh to update it.\n#\n$(cat $f)" > $f
done
sed 's/\\/\\\\/g' test_syncobj3.py > tmp
mv tmp test_syncobj3.py
echo -e "#\n#  WARNING: this is generated file, use generate.sh to update it.\n#\n$(cat test_syncobj3.py)" > test_syncobj3.py

#rm -rf examples_py3/*
#2to3 --output-dir=examples_py3 -W -n examples
#for f in examples_py3/*
#do
#  sed 's/\\/\\\\/g' $f > tmp
#  mv tmp $f
#  echo -e "#!/usr/bin/env python3\n#\n#  WARNING: this is generated file, use gen_py3.sh to update it.\n#\n$(cat $f)" > $f
#done

