# Giza on Cassandra

## Running Tests

1. Set up CloudLab experiment using `cloudconfig.py`. The script allocates the desired number of nodes and creates links connecting them all to a LAN, and also sets the latency of each link, to simulate cross-DC latency. **The latency will be set when configuring an experiment with the profile, and cannot be changed unless you create a new experiment.** It is set to use a pre-created image with Cassandra installed to reduce setup time.
2. Edit `servers.txt` with the SSHs command provided by CloudLab, separated by a new line.
3. On your local machine, run `sync.sh` to copy the repository code to each of the machines.
4. On your local machine, run `python shell.py` to launch a tmux session with SSH connections to each of the machines.
    - `shell.py` provides an easy way to send keystrokes to all of the machines at once.
    - If you are using `shell.py`, all the following commands are to be run on your local machine.
    - Alternatively, you can manually launch SSH connections to each of the machines, and run the commands after `python shell.py send` on each of them while in the `~/giza` directory, replacing `[i]` with the number of the machine (1 for node1 on CloudLab, 2 for node2, etc.). Where it says `'Enter'`, simply press the enter key.

#### Giza Without Contention Tests

5. Run `python shell.py send './setup_giza.sh [i]' 'Enter'` to set up Cassandra on each machine for the Giza tests.
6. Run `python shell.py send './build.sh' 'Enter'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -n 5 -addr 192.168.1.[i] -o 128 -T 1'` to type the test command into each of the tmux panes.

#### Giza With Contention Tests

5. Run `python shell.py send './setup_giza.sh [i]' 'Enter'` to set up Cassandra on each machine for the Giza tests.
6. Run `python shell.py send './build.sh' 'Enter'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -n 5 -addr 192.168.1.[i] -o 1 -T 1'` to type the test command into each of the tmux panes.

#### Cassandra Tests

5. Run `python shell.py send './setup_cassandra.sh [i]' 'Enter'` to set up Cassandra on each machine for the Cassandra tests.
6. Run `python shell.py send './build.sh' 'Enter'` to build Giza on each of the machines.
7. Run `python shell.py send './giza -maddr 192.168.1.1 -m cassandra -addr 192.168.1.[i] -o 128 -T 1'` to type the test command into each of the tmux panes.

#### Finally

8. Press the enter key in node1 (which should be the top left tmux pane), to start the master instance first.
9. Once the master is ready, run `python shell.py send 'Enter'` to start all the other instances.
10. To end the tests, run `python shell.py send 'C-c'` to send the ctrl+c interrupt on all machines.
11. To end the tmux session, run `python shell.py send 'exit' 'Enter' 'exit' 'Enter'`.
12. Copy the latest directory in `results/` and run `copy.sh` to copy all the latency outputs.

## Plotting

Run `python plotFigs/plot_figs.py`. The plots will be generated in `plotFigs/plots/`.


# BCCA 22 notes
- results/17 is giza
- results/18 is giza (maytbe worse)