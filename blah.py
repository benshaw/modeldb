from verta import Client
client = Client("http://localhost:3000")

proj = client.set_project("My first ModelDB project")
expt = client.set_experiment("Default Experiment")

# log the first run
#run = client.set_experiment_run("First Run")
#run.log_hyperparameters({"regularization" : 0.5})
# ... model training code goes here
#run.log_metric('accuracy', 0.72)

# log the second run
#run = client.set_experiment_run("Second Run")
#run.log_hyperparameters({"regularization" : 0.8})
# ... model training code goes here
#run.log_metric('accuracy', 0.83)

name = "test_model"
model = client.get_or_create_registered_model(name=name)
#print(model.stage)