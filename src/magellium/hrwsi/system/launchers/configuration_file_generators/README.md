# Config file generation

The purpose of this section is to generate the configuration file specific to a processing routine.
To do this, we have a **ConfigFileGeneration** parent class and a configuration file template common to all processing routines.
We then have as many child classes as there are processing routines.

The child classes use the following notation:

- {processing_routine_name}ConfigFileGeneration

There are currently the following classes :

- SWSConfigFileGeneration

In the Launcher, depending on the name of the processing routine, the appropriate class is instantiated.

## Adding a new processing routine

Each time a new routine is added, you must :

- Add a class with the fill_conf_yaml function and the appropriate attributes.
- Add the fields for this new processing routine to the config template.
- Add the call to the class in the Launcher
