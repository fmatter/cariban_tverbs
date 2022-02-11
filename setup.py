from setuptools import setup


setup(
    name='cldfbench_cariban_tverbs',
    py_modules=['cldfbench_cariban_tverbs'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'cariban_tverbs=cldfbench_cariban_tverbs:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
