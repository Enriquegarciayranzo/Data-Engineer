# Fundamentals of Data Engineering – Exercise 2 Correction 
### Student: Enrique García
---

## Correction Notes

### Issues to solve:

#### (scrapper) Modify get_songs to use catalog (2 points)
- [ ] Points awarded: 0
- [ ] Comments: You overwritted the 'scrapper/main.py' file. You need to check and test all the code before sending to the final user or going to production.
I added manually a songs folder with some songs to be able to correct the rest of the exercise.

#### (scrapper) Check logs for strange messages (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments: Good job on finding these log entries and finding where they come from.

#### (cleaner) Avoid processing catalogs (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Fix directory creation issue (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:

#### (Validator) Additional validation rule (0.5 points)
- [ ] Points awarded: 0.4
- [ ] Comments: You could have used the 'utils/chords.py' file in the cleaner to not hardcode the chords in the regex. But the rule is good.

#### Code improvements (0.5 points)
- [ ] Points awarded: 0.5
- [ ] Comments:  Good job on handling the missing files.

### Functionalities to add:

#### 'results' module (0.5 points)
- [ ] Points awarded: 0.4
- [ ] Comments: there are a couplpe of issues here:
- - Your code only prints the results. You are not storing anywhere those results. What if it was a pipeline that needs to be executed daily and you need to track these results to monitor them?
- - You are not logging anything from this module. You could use a log file to both log the execution and the results to keep track.

#### 'lyrics' module (2 points)
- [ ] Points awarded: 0.3
- [ ] Comments: The code does not remove all the chords. For example:

```
a_alicia_disfrazada_de_leia_organa.txt

SOL                         RE
tu cerebro se funde con el mío;
SOL                        RE
si sólo fuera porque mi vacío
MIm              LA           RE
lo llenas con tus naves invasoras.
Si sólo fuera porque me enamoras
a golpe de sonámbulo extravío;
si sólo fuera porque en ti confío
```
Also:
- - You are not logging anything from this module.
- - You should have considered using the variables in tab_cleaner/utils/chords.py.
- - You are storing the results in the same validation folder. *This is a critical failure*. You should have created a new directory called 'lyrics' inside the files directory so you can access the lyrics only easily.

#### 'insights' module (2 points)
- [ ] Points awarded:  1.8
- [ ] Comments: Good job here. But you are not logging anything!

#### Main execution file (1 point)
- [ ] Points awarded: 0.5
- [ ] Comments: Works well, but the scrapper file is not working, as I stated avobe.

#### Extra points (2 point)
- [ ] Points awarded: 1.8
- [ ] Comments: Really good job here. The only issue is that the output of the STATS module should be a file, for better use. And you need to log the execution! You could have logged both execution and STATS in the same log file.

---

## Total Score: 7.2 / 10 points

## General Comments:

Good work overall, Enrique. You've demonstrated understanding of the pipeline architecture and implemented most of the required functionalities. However, there are some critical areas that need improvement:

**Main Issues:**
1. **Logging**: This is a recurring problem throughout your submission. Almost none of your modules implement proper logging, which is essential for monitoring, debugging, and tracking pipeline execution in production environments.
2. **Output Management**: Results should be persisted to files, not just printed to console. This is crucial for data pipelines that run on schedules and need audit trails.
3. **File Organization**: The lyrics module stores output in the validation folder instead of creating a dedicated directory. This creates confusion and makes the data harder to access.
4. **Testing**: The scrapper/main.py file was overwritten without proper testing, which would have caught the issue before submission.

**Strengths:**
- Good problem-solving skills, particularly in the insights module
- Clean code structure in most modules
- Effective use of regex for validation rules
- Extra work on the stats module shows initiative

**Recommendations for Future Work:**
- Always implement logging with appropriate levels (INFO, WARNING, ERROR)
- Design clear directory structures for outputs before coding
- Test all components before considering work complete
- Consider reusability (e.g., using existing utility files like chords.py)
- Document your code and architectural decisions

Keep up the good effort, but focus on production-ready practices like logging and proper data management.