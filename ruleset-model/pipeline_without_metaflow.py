from typing import Any



class Pipeline:
    ...


class Step:
    ...



class RulesetPipeline(Pipeline):
    def transform(self, X) -> Any:
        ...


    def fit_transform(self, X, y) -> None:
        ...



class DataLoader(Step):
    def tranform(self, X) -> Any:
        ...


class DataTransformation(Step):
    def transform(self, X) -> Any:
        ...

    def fit_transform(self, X) -> Any:
        ...