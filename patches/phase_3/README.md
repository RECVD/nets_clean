# Prospective Bad Data Patch

## State of the Variables

- Auxiliary Non-hierarchy: Fine
- Auxiliary Hierarchy: Fine except some weird edge cases (zoo and vpa)
- Main as combinations of non-hierarchies: fine
- Main as combinations of hierarchies: Not right.  These should be the same as a simple sum of hierarchy categories within the main categories and they're not

## Possible Scenarios

1. Everything is fine. 
2. Mains should be combinations of Auxiliary hierarchies, which would be like WALH.  But WALH is wrong.  So hierarchies should be re-done (just in case), and then mains re-computed from the hierarchies.

### Does this make sense?

> The hierarchy only uses the auxiliary categories as hierarchy categories. These can be grouped together using the definitions in the main categories to produce the main categories as needed.

What exactly does this mean?