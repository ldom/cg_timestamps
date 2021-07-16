import subprocess


def run_command(command):
    print(command)
    command_as_list = command.split(" ")
    result = subprocess.run(command_as_list, stdout=subprocess.PIPE)
    return result.stdout.decode('utf-8')
