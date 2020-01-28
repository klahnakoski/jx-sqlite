# Logical Equality 

## Definitions

Let `●` represent a logic operator; an operator that returns a Boolean. We define two families of logical operators 

* **conservative** - operators that define `a ● b == null` if either `a` or `b` are `null`
* **decisive** - operators that return only Boolean; do not return `null`


## Desirable features


### Coordinate-wise structural comparison

The conservative `eq` operator, common in databases, is not very useful; a common SQL pattern is `WHERE a=b OR (a IS NULL and b IS NULL)` that is used to detect structural similarity.  We define a new operator, called `eq`, to represent this structural comparison, and we further demand it is decisive.

Given 

    x = {"a": 1, "b": 2}
	y = {"a": 1}
	z = {"c": 3}

We would like 

	{"eq": ["x", y"]} => false
	{"eq": ["z", z"]} => true

Since JSON documents are seen as points in a multidimensional Cartesian space; object comparison should be the same as point comparison; each coordinate can be independently checked for a match; `{"eq": ["x", y"]}` expands to:

    {"and": [
        {"eq":["x.a", "y.a"]}, 
        {"eq":["x.b", "y.b"]},
        {"eq":["x.c", "y.c"]}
    ]}

	⇒

    {"and": [
        true, 
        false,
        true
    ]}

	⇒

	false

and the same logical expansion should work for `z`:  


	{"eq": ["z", z"]}

	⇒

    {"and": [
        {"eq":["z.a", "z.a"]}, 
        {"eq":["z.b", "z.b"]},
        {"eq":["z.c", "z.c"]}
    ]}

	⇒

    {"and": [
        true, 
        true,
        true
    ]}

	⇒

	true

This duck-typing comparison restricts us to only one consistent definition for `eq`:

**Truth table for a *decisive* `eq`**

    {"eq": ["a", "a"]} == true
    {"eq": ["a", "b"]} == false
    {"eq": ["a", null]} == false
    {"eq": ["b", "a"]} == false
    {"eq": ["b", "b"]} == true
    {"eq": ["b", null]} == false
    {"eq": [null, "a"]} == false
    {"eq": [null, "b"]} == false
    {"eq": [null, null]} == true


### Consistency with branching code

Decision code, like `if/else` or `when/then`, are easier to reason about when `not.eq ⇔ ne` 

**Truth table for a *decisive* `ne`**

    {"ne": ["a", "a"]} == false
    {"ne": ["a", "b"]} == true
    {"ne": ["a", null]} == true
    {"ne": ["b", "a"]} == true
    {"ne": ["b", "b"]} == false
    {"ne": ["b", null]} == true
    {"ne": [null, "a"]} == true
    {"ne": [null, "b"]} == true
    {"ne": [null, null]} == false



## Problems with conservative equality`

We will let `==` represent conservative equality

**Truth table for a *conservative* equality**

    {"==": ["a", "a"]} == true
    {"==": ["a", "b"]} == false
    {"==": ["a", null]} == null
    {"==": ["b", "a"]} == false
    {"==": ["b", "b"]} == true
    {"==": ["b", null]} == null
    {"==": [null, "a"]} == null
    {"==": [null, "b"]} == null
    {"==": [null, null]} == null

### Using conservative `==` with decisive `and`: Inconsistent
 
Suppose we want to check that two data structures `x` and `y` are structurally identical. What works for values does not work with `nulls`:

    x = {"a": 1, "b": 2}
	y = {"a": 1}

    {"and": [
        {"eq":["x.a", "y.a"]}, 
        {"eq":["x.b", "y.b"]},
        {"eq":["x.c", "y.c"]}
    ]}

	⇒

    {"and": [
        true, 
        null,
        null
    ]}

	⇒

	# `and` will ignore `null` parameters
    {"and": [
        true
    ]}

	⇒

	true  

Which is the wrong conclusion


### Using conservative `==` with conservative `and`: Inconsistent
 
Given the above, we can conclude that mixing conservative and decisive operators can be dangerous. But sticking to the conservative logical operators is also dangerous:  

    z = {"c": 3}

    {"and": [
        {"eq":["z.a", "z.a"]}, 
        {"eq":["z.b", "z.b"]},
        {"eq":["z.c", "z.c"]}
    ]}

	⇒

    {"and": [
        null, 
        null,
        true
    ]}

	⇒

	null  

which is an unfortunate conclusion

### Using conservative `==` with control flow code: Inconsistent

The conservative operators require care when used in control flow code. What does this expression return?

    CASE WHEN NOT a=b THEN "not equal" ELSE "equal" END

The problem can be seen in the example where `a=1` and `b=NULL`:  

    CASE WHEN NOT 1=null THEN "not equal" ELSE "equal" END
    CASE WHEN NOT null THEN "not equal" ELSE "equal" END
    CASE WHEN null THEN "not equal" ELSE "equal" END
    CASE WHEN null THEN "not equal" ELSE "equal" END
    "equal"

The problem is the control flow statement is decisive: It treats `NULL` as falsey. Our solution is to ensure our logical operators are decisive. 

Maybe we could define conservative control flow: Statements that explicitly deal with `null` or a three-way branching statement. Conservative control flow is not explored here.  

## Summary

The conservative equality `==`, which is used in SQL, is not useful when performing structural comparisons. The decisive `eq` is a more effective choice.