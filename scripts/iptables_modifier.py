from optparse import OptionParser
import paramiko

def main(hosts, operation, user, password):
    hosts = hosts.split(',')
    ssh_connections = []
    for host in hosts:
        _host = host.strip('"')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=_host, username=user, password=password)
        ssh_connections.append(ssh)

    if operation == "close":
        print("Closing ports")
        command_1 = "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT"
        command_2 = "/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT"
        for ssh in ssh_connections:
            ssh.exec_command(command_1)
            ssh.exec_command(command_2)
    elif operation == "open":
        print("Opening ports")
        command = "/sbin/iptables -F"
        for ssh in ssh_connections:
            ssh.exec_command(command)
    else:
        print("Unknown operation!")
        exit(1)

if __name__ == "__main__":
    usage = ''
    parser = OptionParser(usage)
    parser.add_option('-o','--operation', dest='operation')
    parser.add_option('-u','--user', dest='user')
    parser.add_option('-p','--password', dest='password')
    parser.add_option('-i', '--hosts', dest='hosts')

    options, args = parser.parse_args()

    main(options.hosts, options.operation, options.user,
         options.password)
