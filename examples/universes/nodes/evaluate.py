assert isinstance(model, Model)
assert len(history) == 1, 'Another universe leaked into this one'
evaluations = globals().get('evaluations', 0) + 1
prediction = model.predict(3)
print(f'{history[0]}: prediction={prediction}, error={metric(prediction)}')
print(f'Evaluation #{evaluations} in this runtime')
