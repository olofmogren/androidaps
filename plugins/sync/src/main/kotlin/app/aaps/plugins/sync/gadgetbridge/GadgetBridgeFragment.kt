package app.aaps.plugins.sync.gadgetbridge

import dagger.android.support.DaggerFragment
import javax.inject.Inject

class GadgetBridgeFragment: DaggerFragment()
{
    @Inject lateinit var gadgetBridgePlugin: GadgetBridgePlugin

}