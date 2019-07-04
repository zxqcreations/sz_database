
with open('test.csv', 'w') as f:
    for i in range(10):
        f.write(str(i)+','+str(i)+'\n')
with open('test.csv', 'r+') as f:
    lines = f.readlines()
    line = lines[-1]
    print(line)
    f.write('nihao,nihao\n')