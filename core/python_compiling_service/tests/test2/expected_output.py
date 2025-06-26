class UDFCode:
    def process_tuple(self, tuple_: dict, port: int):
        score = 0

        if tuple_["age"] > 18:
            score += 10
            yield (tuple_, 0)

        else:
            score += 5

        if score > 8:
            tuple_["status"] = "high"
            yield (tuple_, 2)