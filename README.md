
### About
This is a cli app that taking some specific rows/columns from a csv file (utf-8 encode) to a plain text file, some columns need format or split_line.  
I write both rust version and python version, and simply compare performance between them on WSL, rust version is only 3.5x faster than python version (3.10).  
Could you help to improve ?  
As Github filesize limited, app_logs_fake.csv < 25MB, you can use: cat app_logs_fake.csv >> app_logs_fake.csv
Update: 1, try use ByteRecord, but it didn't improve much, I think maybe there are too many format!.
2, try multi thread, no improve much, even wrose when csv file is large size, I think the copy of csv reader cause this.
Thanks for @BurntSushi's help!

### Example
```shell
sss@Ubuntu2204:~$ ls -lh app_logs_fake.csv
-rwxrwxr-x 1 sss sss 171M Oct 23 13:35 app_logs_fake.csv
sss@Ubuntu2204:~$ cat app_logs_fake.csv |wc -l
2053713
sss@Ubuntu2204:~$ time python checklog.py app_logs_fake.csv
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
trying to process 1 csv files:
    app_logs_fake.csv
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
csv file: app_logs_fake.csv
AP3 csv logs format verify pass
Output 3 logs:
Log_name            Lines_count         Log_file_path
sequence            47152               app_logs_fake-sequence.log
CONSOLE_11          671537              app_logs_fake-CONSOLE_11.log
SWITCH_01           10377               app_logs_fake-SWITCH_01.log

real    0m3.221s
user    0m1.188s
sys     0m0.141s
sss@Ubuntu2204:~$ time ./checklog app_logs_fake.csv
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
trying to process 1 csv files:
    app_logs_fake.csv
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
csv file: app_logs_fake.csv
BQ4 csv logs format verify pass
Output 3 logs:
Log_name            Lines_count         Log_file_path
sequence            47152               app_logs_fake-sequence.log
CONSOLE_11          671537              app_logs_fake-CONSOLE_11.log
SWITCH_01           10377               app_logs_fake-SWITCH_01.log

real    0m0.864s
user    0m0.219s
sys     0m0.172s
sss@Ubuntu2204:~$
```