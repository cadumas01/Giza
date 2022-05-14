# Giza on Cassandra

## Running Tests

1. Set up CloudLab experiment using `cloudconfig.py`. The script is set to use a pre-created image with Cassandra installed to reduce setup time.
2. Edit `servers.txt` with the SSHs command provided by CloudLab, separated by a new line.
3. Run `sync.sh` to copy the repository code to each of the machines.
4. Run `python shell.py` to launch a tmux session with SSH connections to each of the machines.

#### Giza Without Contention Tests

5. Run `python shell.py send './setup_giza.sh [i]' 'C-m'` to set up Cassandra on each machine for the Giza tests.
6. Run `python shell.py send './build.sh' 'C-m'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -n 5 -addr 192.168.1.[i] -o 128 -T 1'` to enter the command in each of the tmux panes.

#### Giza With Contention Tests

5. Run `python shell.py send './setup_giza.sh [i]' 'C-m'` to set up Cassandra on each machine for the Giza tests.
6. Run `python shell.py send './build.sh' 'C-m'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -n 5 -addr 192.168.1.[i] -o 1 -T 1'` to enter the command in each of the tmux panes.

#### Cassandra Tests

5. Run `python shell.py send './setup_cassandra.sh [i]' 'C-m'` to set up Cassandra on each machine for the Cassandra tests.
6. Run `python shell.py send './build.sh' 'C-m'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -m cassandra -addr 192.168.1.[i] -o 128 -T 1'` to enter the command in each of the tmux panes.

#### Finally

8. Press the enter key in the top left tmux pane, to start the master instance first.
9. Once the master is ready, run `python shell.py send 'C-m'` to start all the other instances.
10. To end the tests, run `python shell.py send 'C-c'`.
11. To end the tmux session, run `python shell.py send 'exit' 'C-m' 'exit' 'C-m'`.
12. Copy the latest directory in `results/` and run `copy.sh` to copy all the latency outputs.

## Plotting

Run `python plotFigs/plot_figs.py`. The plots will be generated in `plotFigs/plots/`.
