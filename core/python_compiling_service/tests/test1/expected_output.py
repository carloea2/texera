class UDFCode:
    def process_tuple(self, tuple_: dict, port: int):
        score = 0
        category = "unknown"

        if tuple_["age"] > 18:
            score += 10
            category = "adult"
            yield (tuple_, 0)

        else:
            score += 5
            category = "minor"

        if score > 8:
            tuple_["status"] = "high"
            yield (tuple_, 2)

        else:
            tuple_["status"] = "low"
            
        tuple_["category"] = category
        tuple_["score"] = score
        yield (tuple_, 4)