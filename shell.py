import math
import os
import sys

servers_file = "./servers.txt"
shell = "zsh"

with open(servers_file, "r") as f:
    servers = f.readlines()

if len(servers) == 0:
    print("Please put servers in servers.txt, separated by a new line")
    exit(1)

if len(sys.argv) > 1:
    if sys.argv[1] == "send" and len(sys.argv) > 2:
        script = "tmux select-window -t gshell:0"
        args = sys.argv[2:]
        args = [f"'{arg}'" for arg in args]
        args = " ".join(args)
        print(args)
        for i in range(len(servers)):
            a = args.replace("[i]", str(i+1))
            script += f" \; send-keys -t {i} {a}"
        print(script)
        os.system(script)
        exit(0)
    try:
        si = int(sys.argv[1])
        if si >= 1 and si <= len(servers):
            os.system(servers[si - 1])
            exit(0)
        else:
            print("Invalid index")
            exit(1)
    except:
        print("Invalid index")
        exit(1)

script = ""

for i in range(len(servers)):
    if i == 0:
        script = f"tmux new-session -s gshell '{shell}' \; rename-window 'Giza Shell'"
    elif i == 1:
        script += f" \; split-window -v '{shell}'"
    else:
        step = math.floor(math.log(i, 2))
        upper = (2 ** (step + 1)) - 1

        if step % 2 == 0:
            spdir = "-v"
        else:
            spdir = "-h"

        script += f" \; split-window {spdir} -t {upper - i} '{shell}'"

for i, server in enumerate(servers):
    script += f" \; send-keys -t {i} '{server}' 'C-m' 'cd giza' 'C-m' 'clear' 'C-m'"

os.system(script)

# while IFS= read -r line
# do
#   if test $n -eq 0
#   then
#     script="tmux new-session '$line ; bash'"
#   else
#     script="$script \; split-window '$line ; bash'"
#   fi
# done < "$servers"
# 
# echo $script
