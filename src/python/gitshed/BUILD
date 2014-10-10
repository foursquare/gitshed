python_library(name='lib',
               sources=globs('*.py') - ['main.py'],
               dependencies=[
                 '3rdparty:click',
                 '3rdparty:requests',
               ]
)

python_binary(name='gitshed',
              source='main.py',
              dependencies=[
                ':lib'
              ]
)