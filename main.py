import sqlite3

from orm import run
from orm import model
from orm import core


class Student(model.Table):
    id = model.TextField(primary_key=True)
    name = model.TextField(unique=True)
    year = model.IntegerField()
    major = model.TextField(not_null=False)
    
    def __str__(self):
        return str(f"Student {self.id}\nName:\t{self.name}\nYear:\t{self.year}\nMajor:\t{self.major}")

lawson = Student(id="901", name="Lawson Milwood", year=2024, major="Straight Science")

con = sqlite3.connect("test.db")
orm = core.Engine(con)

orm.save(lawson)