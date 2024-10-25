text = list(input())

i = 0
j = len(text) - 1

while (i < j):
    temp_char = text[i]
    text[i] = text[j]
    text[j] = temp_char
    i += 1
    j -= 1

print(''.join(text))