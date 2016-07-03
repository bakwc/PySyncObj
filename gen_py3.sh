#!/usr/bin/env bash
rm -rf pysyncobj/pysyncobj3/
2to3 --output-dir=pysyncobj3 -W -n pysyncobj
mv pysyncobj3/ pysyncobj/pysyncobj3/
mv pysyncobj/pysyncobj3/py3init.py pysyncobj/pysyncobj3/__init__.py
cp syncobj_ut.py syncobj3_ut.py
2to3 -w syncobj3_ut.py

for f in pysyncobj/pysyncobj3/*
do
  sed 's/\\/\\\\/g' $f > tmp
  mv tmp $f
  echo -e "#\n#  WARNING: this is generated file, use gen_py3.sh to update it.\n#\n$(cat $f)" > $f
done
sed 's/\\/\\\\/g' syncobj3_ut.py > tmp
mv tmp syncobj3_ut.py
echo -e "#\n#  WARNING: this is generated file, use gen_py3.sh to update it.\n#\n$(cat syncobj3_ut.py)" > syncobj3_ut.py
