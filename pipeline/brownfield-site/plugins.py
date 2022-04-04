import csv
import re

import digital_land


class HarmoniserPlugin:
    organisation_uri = {}
    end_of_uri_regex = re.compile(r".*/")

    @digital_land.hookimpl
    def init_harmoniser_plugin(self, harmoniser):
        self.harmoniser = harmoniser

        for row in csv.DictReader(open("var/cache/organisation.csv", newline="")):
            if "opendatacommunities" in row:
                uri = row["opendatacommunities"].lower()
                value = row["organisation"]
                self.organisation_uri[row["organisation"].lower()] = value
                self.organisation_uri[uri] = value
                self.organisation_uri[self.end_of_uri(uri)] = value
                self.organisation_uri[row["statistical-geography"].lower()] = value
                if "local-authority-eng" in row["organisation"]:
                    dl_url = "https://digital-land.github.io/organisation/%s/" % (
                        row["organisation"]
                    )
                    dl_url = dl_url.lower().replace("-eng:", "-eng/")
                    self.organisation_uri[dl_url] = value

        self.organisation_uri.pop("")

    @digital_land.hookimpl
    def apply_patch_post(self, fieldname, value):
        if fieldname == "organisation":
            normalised_value = self.lower_uri(value)

            if normalised_value in self.organisation_uri:
                return self.organisation_uri[normalised_value]

            s = self.end_of_uri(normalised_value)
            if s in self.organisation_uri:
                return self.organisation_uri[s]

            self.harmoniser.log_issue(
                fieldname, "opendatacommunities-uri", normalised_value
            )
        return value

    def lower_uri(self, value):
        return "".join(value.split()).lower()

    def end_of_uri(self, value):
        return self.end_of_uri_regex.sub("", value.rstrip("/").lower())


# regsiter plugin instances, not the classes themselves
harmoniser_plugin = HarmoniserPlugin()
