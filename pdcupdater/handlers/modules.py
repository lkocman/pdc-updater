import logging
import re

import beanbag
import pdcupdater.handlers
import pdcupdater.services
import rida


log = logging.getLogger((__name__))

def get_koji_tag(variant_info):
    return "%s-%s-%s" % (variant_info['variant_id'], variant_info['variant_version'], variant_info['variant_release'])

class NewUnreleasedVariantHandler(pdcupdater.handlers.BaseHandler):
    """ When a new tree is created """

    relevant_states = [rida.BUILD_STATES['init']]
    valid_states = relevant_states + [rida.BUILD_STATES[s] for s in ('wait', 'failed', 'done', 'ready')]

    @property
    def topic_suffixes(self):
        return ['rida.module.state.change']

    def can_handle(self, msg):
        if not msg['topic'].endswith('rida.module.state.change'):
            log.debug("ignoring msg %s" % msg)
            return False
        log.debug("getting state from msg %s" % msg)
        state = int(msg['msg']['state'])

        if state not in self.valid_states:
            log.warn("Invalid module state '{}', skipping.".format(state))
            return False

        if state not in self.relevant_states:
            log.debug("Non-relevant module state '{}', skipping.".format(
                state))
            return False

        return True


    def _get_deps_from_msg(self, msg, attr):
        deps = msg['msg'].get(attr, None)
        result =[]

        if not deps:
            return result

        for module, version_str in deps.iteritems():
            if not version_str: # no release-version was specified
                result.append({'dependency': module})
                continue

            first_num = re.search("\d", version_str).start()
            condition = version_str[:first_num].strip() # '' if none (don't add space)
            if condition in ('==', '='):
                condition = '' # let's store just 'module-1.2' instead of '== module-1.2'

            # if we'd still have condition
            if condition:
                condition += " " # add space in front of e.g. '>= module-1.2' rather than '>=module-1.2'

            version_release = version_str[first_num:].strip()
            result.append({'dependency' : "%s%s-%s" % (condition, module, version_release)})

        return result

    def get_runtime_deps_from_msg(self, msg):
        return self._get_deps_from_msg(msg, attr='requires')

    def get_build_deps_from_msg(self, msg):
        return self._get_deps_from_msg(msg, attr='buildrequires')

    def handle(self, pdc, msg):
        #state = msg['msg']['state']
        log.info("handling message")
        variant_info = {
            'variant_id': msg['msg']['name'],
            'variant_uid': msg['msg']['name'],
            'variant_name': msg['msg']['name'],
            'variant_version':msg['msg']['version'],
            'variant_release': msg['msg']['release'],
            'variant_type': 'module',
        }
        variant_info['koji_tag'] = get_koji_tag(variant_info)
        variant_info['build_deps'] =  self.get_runtime_deps_from_msg(msg)
        variant_info['runtime_deps'] =  self.get_runtime_deps_from_msg(msg)
        try:
            unreleased_variant = pdc.unreleasedvariants._(variant_info)
            log.info("unreleased_variant %s" % unreleased_variant)
        except beanbag.BeanBagException as e:
            if e.response.status_code != 404:
                log.error(e.response.text)
                raise
            unreleased_variant = pdc['unreleasedvariants']._(variant_info)
        log.info(variant_info)

    def audit(self, pdc):
        pass

    def initialize(self, pdc):
        pass
