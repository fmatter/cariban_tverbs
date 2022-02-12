import pathlib
import pandas as pd
from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from cldf_helpers import flatten_list, custom_spec, split_ref
from clldutils.misc import slug
import cariban_helpers as crh
import pybtex
from pycldf.sources import Source


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "cariban_tverbs"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir="cldf", module="Wordlist", metadata_fname="metadata.json")

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        pass

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """
        forms = pd.read_csv("raw/forms.csv", keep_default_na=False)
        cognatesets = pd.read_csv("raw/cognates.csv", keep_default_na=False)
        cognates = [
            {
                "ID": f"""{x["ID"]}-{x["Cognateset_ID"]}""",
                "Form_ID": x["ID"],
                "Cognateset_ID": x["Cognateset_ID"],
            }
            for i, x in forms.iterrows()
        ]
        forms.drop(columns=["Cognateset_ID"], inplace=True)

        meanings = flatten_list(
            [row["Meaning"].split("; ") for i, row in forms.iterrows()]
        )
        cogmeanings = flatten_list(
            [row["Meaning"].split("; ") for i, row in cognatesets.iterrows()]
        )
        meanings = meanings + cogmeanings
        repl = {
            "eat (meat)": "eat meat",
            "eat (bread)": "eat bread",
            "throw (out)": "throw out",
            "eat (starch)": "eat starch",
            "gather (fruit)": "gather fruit",
            "shoot (blowgun)": "shoot blowgun",
            "light (fire)": "light fire",
        }
        meanings = [x if x not in repl else repl[x] for x in meanings]
        meanings = list(set(meanings))
        meanings = [{"ID": slug(x), "Name": x} for x in meanings]
        meaning_dic = {x["Name"]: x["ID"] for x in meanings}
        for a, b in repl.items():
            meaning_dic[a] = meaning_dic[b]

        # args.writer.cldf.add_component("FormTable")
        args.writer.cldf.add_component("ParameterTable")
        args.writer.cldf.add_component("LanguageTable")
        # args.writer.cldf.add_component("CodeTable")
        args.writer.cldf.add_component("CognatesetTable")
        args.writer.cldf.add_component("CognateTable")

        args.writer.cldf.remove_columns("FormTable", "Parameter_ID")
        args.writer.cldf.add_columns(
            "FormTable",
            custom_spec("FormTable", "Parameter_ID", separator="; "),
        )

        def repl_lg(glottocode):
            if glottocode == "kuik1246":
                return "uxc"
            return crh.get_lg_id(glottocode)

        lgs = []
        for i, row in forms.iterrows():
            lg = repl_lg(row["Language_ID"])
            row["Language_ID"] = lg
            row["Source"] = [row["Source"]]
            parameters = [meaning_dic[x] for x in row["Meaning"].split("; ")]
            row["Parameter_ID"] = parameters
            args.writer.objects["FormTable"].append(row)
            lgs.append(lg)

        lgs.append("PC")
        languages = crh.get_cldf_lg_table(lgs)

        # PC forms are stored in cognatesets table...
        for i, row in cognatesets.iterrows():
            pc_id = "pc-" + str(row["ID"])
            parameters = [slug(x) for x in row["Meaning"].split("; ")]
            args.writer.objects["FormTable"].append(
                {
                    "Language_ID": "PC",
                    "ID": pc_id,
                    "Form": "*" + row["Form"],
                    "Parameter_ID": parameters,
                    "Source": [row["Source"]],
                }
            )
            args.writer.objects["CognateTable"].append(
                {"Form_ID": pc_id, "ID": row["ID"], "Cognateset_ID": row["ID"]}
            )
            args.writer.objects["CognatesetTable"].append(
                {"ID": row["ID"], "Description": row["Description"]}
            )

        for row in meanings:
            args.writer.objects["ParameterTable"].append(row)

        for row in cognates:
            args.writer.objects["CognateTable"].append(row)

        for row in languages:
            args.writer.objects["LanguageTable"].append(row)

        # fetch found bibkeys from sources.bib
        found_refs = []
        for df in [forms, cognatesets]:
            if "Source" in df.columns:
                for i in df["Source"]:
                    if pd.isnull(i):
                        continue
                    for r in i.split("; "):
                        found_refs.append(split_ref(r)[0])
        found_refs = list(set(found_refs))
        found_refs.remove("pc")

        bib = pybtex.database.parse_file("raw/sources.bib", bib_format="bibtex")
        sources = [
            Source.from_entry(k, e) for k, e in bib.entries.items() if k in found_refs
        ]
        pc = pybtex.database.Entry(
            "misc",
            [
                ("title", "Placeholder for data obtained from personal communication."),
            ],
        )
        sources.append(Source.from_entry("pc", pc))
        args.writer.cldf.add_sources(*sources)

        args.writer.write()
