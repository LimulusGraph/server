from dataclasses import dataclass

@dataclass
class Model:
    scale: float

    def predict(self, x):
        return self.scale * x


def make_metric(target):
    return lambda value: abs(value - target)


model = Model(scale=1)
metric = make_metric(6)
history = []
print('Created a model and closure in this universe')
