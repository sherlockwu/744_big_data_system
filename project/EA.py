from random import choice, randint, random
from string import lowercase
from Solid.EvolutionaryAlgorithm import EvolutionaryAlgorithm

import helper

Nm  = 0
Ns  = 0
n   = []
DAG = []

# member
#[
# [.....]
# [.....]
#]

class EA_Optimizer(EvolutionaryAlgorithm):
    def _initial_population(self):
        global Nm, Ns, n, DAG
        return list( helper.random_generate_one(Nm, Ns, n) for _ in range(50))   #TODO

    def _fitness(self, member):
        global Nm, Ns, n, DAG

        return float( 0 - helper.calculate_score(member, DAG) )      #TODO

    def _crossover(self, parent1, parent2):
        global Nm, Ns, n, DAG
        partition = randint(1, Ns - 1)
        return parent1[0:partition] + parent2[partition:]

    def _mutate(self, member):
        global Nm, Ns, n, DAG

        if self.mutation_rate >= random():
            for i in range(Ns-1):     # try to mutate each task  TODO
                if self.mutation_rate >= random():
                    member[i] = helper.random_generate_one_task( Nm, n[i] )

        return member


def EA_Handler( Nm_, Ns_, n_, DAG_ ):
    global Nm, Ns, n, DAG
    Nm  = Nm_
    Ns  = Ns_
    n   = n_
    DAG = DAG_
    print len(DAG)
#    for i in range(len(DAG)):
#        print "This node: ", DAG[i].run_time_, " parent: ", DAG[i].parent_
    algorithm = EA_Optimizer(.5, .5, 50, max_fitness=None)
    return algorithm.run()

