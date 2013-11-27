__author__ = 'VHASFCFLENND'

last_file = r'\\sfc-9lrba_vs1\MRSU_Central\Database\Transfers\Incoming\MAC\CVLT-II\ToImport\ExportData10_22_2012.txt'
new_file = r'\\sfc-9lrba_vs1\MRSU_Central\Database\Transfers\Incoming\MAC\CVLT-II\ToImport\11_20_2012 export.txt'

output = r'\\sfc-9lrba_vs1\MRSU_Central\Database\Transfers\Incoming\MAC\CVLT-II\ToImport\ExportData.txt'
last_f = open(last_file, 'r')
new_f = open(new_file, 'r')
output_f = open(output, 'w')

last_lines = last_f.readlines()
for line in new_f:
    if line in last_lines:
        continue
    else:
        output_f.write(line)

output_f.close()
last_f.close()
new_f.close()

