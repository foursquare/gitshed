python_library(name='lib',
               sources=globs('*.py') - ['main.py'],
               dependencies=[
                 '3rdparty/python:click',
               ]
)

python_binary(name='gitshed',
              source='main.py',
              dependencies=[
                ':lib'
              ]
)