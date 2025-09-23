package app.aaps.plugins.sync.banglejs

import dagger.android.support.DaggerFragment
import javax.inject.Inject

class BangleJSFragment: DaggerFragment()
{
    @Inject lateinit var bangleJSPlugin: BangleJSPlugin

}