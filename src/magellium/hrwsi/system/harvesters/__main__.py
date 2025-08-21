from magellium.hrwsi.system.harvesters.application.ports.inputs.user_interface import UserInterface
from magellium.hrwsi.system.harvesters.infrastructure.adapters.inputs.user_interface import CommandLineUserInterface

def main():
    user_interface: UserInterface = CommandLineUserInterface()
    try:
        user_interface.start()
    except KeyboardInterrupt:
        user_interface.stop()

if (__name__ == "__main__"):
    main()