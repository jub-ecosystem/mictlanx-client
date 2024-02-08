from pyparsing import Word,alphas,alphanums,nums,Combine,Suppress,Optional,Literal,Group,StringStart,StringEnd,OneOrMore,CaselessLiteral,oneOf
import string

WHITESPACE = Suppress(" ")
SEMICOLON = Suppress(":")
DOT       = Literal(".")
SEMICOLON_IGNORING_SPACES = Optional(WHITESPACE)+SEMICOLON+Optional(WHITESPACE)
NUMBERS = Word(nums)
# kv = Word(alphas) + ":"+Word(alphanums)

identifier = Word(alphanums+"-_")
# Combine(Word(alphanums)+Optional(oneOf("_ -") )+Word(alphanums) )
# OneOrMore(Word(alphanums) +  + Word(alphanums))
who_value = identifier

who = Group(Literal("Who")+SEMICOLON_IGNORING_SPACES+who_value)

what_value = OneOrMore(Suppress("-")+identifier )
what = Group(Literal("What")+SEMICOLON_IGNORING_SPACES+what_value)

where_value = OneOrMore(Suppress("-")+identifier )
where = Group(Literal("Where")+SEMICOLON_IGNORING_SPACES+what_value)

how_value = CaselessLiteral("ACTIVE")
how = Group(Literal("How")+SEMICOLON_IGNORING_SPACES+how_value)

replication_predicate_variable = Word(string.ascii_uppercase+"-_")
replication_predicate_value_decimal = Combine( NUMBERS+Optional(DOT+ NUMBERS) )
replication_predicate_value_percentage = Combine(NUMBERS+Optional(DOT+NUMBERS))+Literal("%") 

replication_predicate = Group(Suppress("$") + Optional(Suppress("."))+replication_predicate_variable+oneOf("> < >= <= !=")+ (replication_predicate_value_percentage | replication_predicate_value_decimal)  )
# when_value_value = Group(replication_predicate)
when_value = Group(OneOrMore(Group(Suppress("-")+Word(alphanums) +SEMICOLON_IGNORING_SPACES+ replication_predicate)))
when = Group(Literal("When")+SEMICOLON_IGNORING_SPACES+when_value)

version = Combine(Literal("v")+Word(nums))
tlaloc_version = Group(Literal("tlaloc")+SEMICOLON_IGNORING_SPACES+version )
ap = StringStart() + tlaloc_version +who+what+where+how+when+ StringEnd()
test_string = """
    tlaloc:v1
    Who: peer0
    What:
        - bucket-0l
        - bucket-02
        - bucket-03
    Where:
        - peer-1
        - peer-22
    How: AcTivE
    When:
        -bucket0:$ACCESS_FREQUENNCY>10
        -bucket0:$ACCESS_FREQUENNCY>60.6%
"""

print(ap.parseString(test_string))
# tlaloc_version_str ="""
    # tlaloc:v1
# """
# print(replication_predicate.parseString("""$ACCESS_FREQUENCY > 100.0%"""))
# key_str = "mictlanx-peer-0"
# print(identifier.parseString(key_str))
# print(ap.parseString(test_string))
# print(tlaloc_version.parseString(tlaloc_version_str))
# tlaloc: v1
# print(who.parseString(test_string))
# availability_policy = 
